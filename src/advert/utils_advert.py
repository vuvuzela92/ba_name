import os
import aiohttp
import asyncio
from datetime import datetime, timedelta
import gspread
from time import time
import logging
from dotenv import load_dotenv
import json
import pandas as pd
import requests
import itertools

load_dotenv()

def load_api_tokens():
    # Проверяем наличие файла tokens.json во всех директориях проекта
    current_dir = os.path.dirname(os.path.abspath(__file__))

    while True:
        tokens_path = os.path.join(current_dir, 'tokens.json')
        if os.path.isfile(tokens_path):
            try:
                with open(tokens_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                print(f"Ошибка декодирования JSON в файле: {tokens_path}")
                return None

        # Поднимаемся на уровень выше
        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir:
            # Достигли корня диска
            break
        current_dir = parent_dir

    print("Файл tokens.json не найден ни в одной из директорий")
    return None

def safe_open_spreadsheet(title, retries=5, delay=5):
    """
    Пытается открыть таблицу с повторными попытками при APIError 503.
    """
    gc = gspread.service_account(filename=os.path.join(os.path.dirname(__file__), 'creds.json'))
    
    for attempt in range(1, retries + 1):
        logging.info(f"[Попытка {attempt}] открыть доступ к таблице '{title}'")
        
        try:
            spreadsheet = gc.open(title)
            logging.info(f"✅ Таблица '{title}' успешно открыта")
            return spreadsheet
            
        except gspread.APIError as e:
            error_code = e.response.status_code if hasattr(e, 'response') else None
            logging.info(f"⚠️ [Попытка {attempt}/{retries}] APIError {error_code}: {e}")
            
            if error_code == 503:
                if attempt < retries:
                    logging.info(f"⏳ Ожидание {delay} секунд перед повторной попыткой...")
                    time.sleep(delay)
                    # Увеличиваем задержку для следующей попытки (exponential backoff)
                    delay *= 2
                else:
                    logging.error("❌ Все попытки исчерпаны")
                    raise
            else:
                # Другие ошибки API (403, 404 и т.д.) - не повторяем
                raise
                
        except gspread.SpreadsheetNotFound:
            logging.info(f"❌ Таблица '{title}' не найдена")
            raise
            
        except Exception as e:
            logging.error(f"⚠️ [Попытка {attempt}/{retries}] Неожиданная ошибка: {e}")
            if attempt < retries:
                logging.error(f"⏳ Ожидание {delay} секунд...")
                time.sleep(delay)
                delay *= 2
            else:
                raise RuntimeError(f"Не удалось открыть таблицу '{title}' после {retries} попыток.")
            
def send_df_to_google(df, sheet):
    """
    Отправляет DataFrame на указанный лист Google Таблицы.

    Параметры:
    df (DataFrame): DataFrame, который нужно отправить.
    sheet (gspread.models.Worksheet): Объект листа, на который будут добавлены данные.

    Возвращаемое значение:
    None
    """
    try:
        # Данные, которые нужно добавить
        df_data_to_append = [df.columns.values.tolist()] + df.values.tolist()
        
        # Проверка существующих данных на листе
        existing_data = sheet.get_all_values()
        
        if len(existing_data) <= 1:  # Если данных нет
            print("Добавляем заголовки и данные")
            sheet.append_rows(df_data_to_append, value_input_option='USER_ENTERED')
             # Получаем текущую дату и время
            now = datetime.now()
            formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
            
            # Получаем количество колонок на листе
            max_columns = sheet.col_count
            
            # Записываем дату и время в первую строку последней колонки
            sheet.update_cell(1, max_columns, formatted_time)
            print(f"Дата и время последнего обновления: {formatted_time}")
        else:
            print("Добавляем только данные")
            sheet.append_rows(df_data_to_append[1:], value_input_option='USER_ENTERED')
            now = datetime.now()
            formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
            
            # Получаем количество колонок на листе
            max_columns = sheet.col_count
            
            # Записываем дату и время в первую строку последней колонки
            sheet.update_cell(1, max_columns, formatted_time)
            print(f"Дата и время последнего обновления: {formatted_time}")
            
    except Exception as e:
        print(f"An error occurred: {e}")

# === Для ежедневной воронки
def batchify(data, batch_size):
    """
    Splits data into batches of a specified size.

    Parameters:
    - data: The list of items to be batched.
    - batch_size: The size of each batch.

    Returns:
    - A generator yielding batches of data.
    """
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]    


semaphore = asyncio.Semaphore(10)
async def adv_stat_async(campaign_ids: list, date_from: str, date_to: str, api_token: str, account: str):
    """
    Получение статистики по списку ID кампаний за указанный период.

    :param campaign_ids: список ID кампаний
    :param date_from: дата начала периода в формате YYYY-MM-DD
    :param date_to: дата окончания периода в формате YYYY-MM-DD
    :param api_token: токен для API WB
    :param account: название аккаунта
    """
    url = "https://advert-api.wildberries.ru/adv/v3/fullstats"
    headers = {"Authorization": api_token}
    batches = list(batchify(campaign_ids, 100))
    data = []
    async with semaphore:
        async with aiohttp.ClientSession(headers=headers) as session:
            for batch in batches:
                ids_str = ",".join(str(c) for c in batch)
                params = {"ids": ids_str, "beginDate": date_from, "endDate": date_to}

                print(f"Запрос для {account}: {params}")

                retry_count = 0
                while retry_count < 5:
                    try:
                        async with session.get(url, params=params) as response:
                            print(f"HTTP статус: {response.status}")

                            if response.status == 400:
                                err = await response.json()
                                print(f"Ошибка 400 {account}: {err.get('message') or err}")
                                # retry_count += 1
                                # await asyncio.sleep(60)
                                continue

                            if response.status == 429:
                                print("429 Too Many Requests — ждём минуту")
                                retry_count += 1
                                await asyncio.sleep(60)
                                continue

                            response.raise_for_status()
                            batch_data = await response.json()

                            # добавляем поле account в каждый элемент
                            for item in batch_data or []:
                                item["account"] = account
                                item["date"] = date_from
                            data.extend(batch_data or [])
                            break

                    except aiohttp.ClientError as e:
                        print(f"Сетевая ошибка для {account}: {e}")
                        retry_count += 1
                        await asyncio.sleep(30)

                # WB ограничивает 1 запрос/мин → ждём после каждого батча
                await asyncio.sleep(60)

        return data
    

def camp_list(api_token: str, account: str):
    url = 'https://advert-api.wildberries.ru/adv/v1/promotion/adverts'
    camps = []
    campaign_statuses = [9, 11]
    headers = {'Authorization': api_token}
    for status_id in campaign_statuses:
        params = {
        'status': status_id,
        'order': 'id'
                }
        payload = []
        try:
            res = requests.post(url, headers=headers, params=params, json=payload)
            res.raise_for_status()
            data = res.json()
        except Exception as e:
            print(e)
            data = []

        if data:
                # Добавляем информацию о кабинете в данные
                for item in data:
                    item['account'] = account
                camps.append(data)
    return camps


def camp_list_manual(api_token: str, account: str):
    url = 'https://advert-api.wildberries.ru/adv/v0/auction/adverts'
    camps = []
    campaign_statuses = [9, 11]
    headers = {'Authorization': api_token}
    for status_id in campaign_statuses:
        params = {
        'status': status_id
                }
        try:
            res = requests.get(url, headers=headers, params=params)
            res.raise_for_status()
            data = res.json()
        except Exception as e:
            print(e)
            data = []

        if data:
                # Добавляем информацию о кабинете в данные
                for item in data['adverts']:
                    item['account'] = account
                camps.append(data['adverts'])
    return camps    

async def get_all_adv_data(days_count=1):
    """ Получаем по ручной и единой РК"""
    all_adv_data = []
    tasks = []
    for account, api_token in load_api_tokens().items():
        # Получаем информацию об РК с единой ставкой
        camps_list = camp_list(api_token, account)
        # Преобразуем список списков в один список
        campaigns = list(itertools.chain(*camps_list))
        campaign_ids = [c['advertId'] for c in campaigns]
        # Получаем информацию об РК с ручной ставкой
        camps_list_2 = camp_list_manual(api_token, account)
        # Преобразуем список списков в один список
        campaigns_2 = list(itertools.chain(*camps_list_2))
        campaign_ids_2 = [c['id'] for c in campaigns_2 if c['status'] in (9, 11)]
        # Объединяем списки РК в единый
        campaign_ids.extend(campaign_ids_2)
        # Убираем дубликаты
        campaign_ids = list(set(campaign_ids))
        for day in range(1, days_count+1):
            yesterday = datetime.now() - timedelta(days=day)
            date_from = date_to = yesterday.strftime("%Y-%m-%d")
            print(f"Получаем данные за {date_from} по ЛК {account}")
            # date_range = [date_from]
            tasks.append(adv_stat_async(campaign_ids, date_from, date_to, api_token, account))
    # Получаем статистику по кампаниям
    stats = await asyncio.gather(*tasks)
    for stat in stats:
        all_adv_data.extend(stat)
    return all_adv_data

def processed_adv_data(adv_data):
    processed_data = []
    for camp in adv_data:
        # Для АРК берем среднюю позицию из boosterStats
        try:
            camp['avg_position'] = camp['boosterStats'][0]['avg_position']
        except (KeyError, IndexError):
            camp['avg_position'] = None
        # Получаем данные по всем платформам ios, PC, android
        try:
            platforms = camp['days'][0]['apps']
            for platform in platforms:
                # Если appType = 1, то это ПК
                if platform['appType'] == 1:
                    camp['atbs_pc'] = platform['atbs']
                    camp['canceled_pc'] = platform['canceled']
                    camp['clicks_pc'] = platform['clicks']
                    camp['cpc'] = platform['cpc']
                    camp['cr_pc'] = platform['cr']
                    camp['ctr_pc'] = platform['ctr']
                    camp['orders_pc'] = platform['orders']
                    camp['shks_pc'] = platform['shks']
                    camp['sum_price_pc'] = platform['sum_price']
                    camp['views_pc'] = platform['views']
                    camp['article_id'] = platform['nms'][0]['nmId']
                # Если appType = 32, то это андроид
                elif platform['appType'] == 32: 
                    camp['atbs_android'] = platform['atbs']
                    camp['canceled_android'] = platform['canceled']
                    camp['clicks_android'] = platform['clicks']
                    camp['cr_android'] = platform['cr']
                    camp['ctr_android'] = platform['ctr']
                    camp['orders_android'] = platform['orders']
                    camp['shks_android'] = platform['shks']
                    camp['sum_price_android'] = platform['sum_price']
                    camp['views_android'] = platform['views']
                    camp['article_id'] = platform['nms'][0]['nmId']
                elif platform['appType'] == 64:  # Если appType = 4, то это ios
                    camp['atbs_ios'] = platform['atbs']
                    camp['canceled_ios'] = platform['canceled']
                    camp['clicks_ios'] = platform['clicks']
                    camp['cr_ios'] = platform['cr']
                    camp['ctr_ios'] = platform['ctr']
                    camp['orders_ios'] = platform['orders']
                    camp['shks_ios'] = platform['shks']
                    camp['sum_price_ios'] = platform['sum_price']
                    camp['views_ios'] = platform['views']
                    camp['article_id'] = platform['nms'][0]['nmId']
        except KeyError:
            print('no days key')
        
        # Удаляем ненужные ключи, перед созданием датафрейма
        try:
            del camp['boosterStats']
        except KeyError:
            print("Нет ключа boosterStats")
        # 
        try:
            del camp['days']
        except KeyError:
            print("Нет ключа boosterStats")
        processed_data.append(camp)
    return processed_data