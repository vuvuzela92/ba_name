import os
from datetime import datetime, timedelta
import gspread
from time import time
import logging
from dotenv import load_dotenv
import json
import pandas as pd
import requests
from time import sleep

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

def get_adv_spend(account, date_from, date_to, headers):
    params = {'from': date_from, 'to': date_to}
    # Тело запроса рекламных затрат
    url = 'https://advert-api.wildberries.ru/adv/v1/upd'
    task_completed = False
    results = {}
    retry_count = 0
    max_retries = 5
    while not task_completed and retry_count < max_retries:
            res = requests.get(url, headers=headers, params=params)
            try:
                res.raise_for_status()  # Проверка на ошибки HTTP
                results = pd.DataFrame(res.json())  
                results['account'] = account
                results['updTime'] = pd.to_datetime(results['updTime'], format='ISO8601').dt.date.astype(str)
                results['updTime'] = results['updTime'].loc[results['updTime'] == date_from]
                task_completed = True
            except requests.exceptions.RequestException as e:
                print(f"Error for account {account}: {e}")
                retry_count += 1
                sleep(20)
    if task_completed:           
        return results
    else:
        print(f"Превывышено максимальное количество запросов для {account}.")

def processed_adv_spend(days_count=1):
    adv_spend_list = []
    for day in range(1, days_count+1):
        yesterday = datetime.now() - timedelta(days=day)
        date_from = date_to = yesterday.strftime("%Y-%m-%d")        
        for account, api_token in load_api_tokens().items():
            headers = {
                    "Authorization": api_token
                }
            adv_spend_list.append(get_adv_spend(account, date_from, date_to, headers))
        
    adv_spend_df = pd.concat(adv_spend_list, ignore_index=True, axis='rows')

    adv_spend_df['updTime'] = pd.to_datetime(adv_spend_df['updTime'], format='ISO8601').dt.date.astype(str)
    adv_spend_df['sku'] = adv_spend_df['campName'].apply(lambda x : x[:9])
    adv_spend_df = adv_spend_df[['updTime', 'campName', 'paymentType', 'updNum', 'updSum', 'advertId', 'advertType', 'advertStatus', 'sku', 'account']]
    return adv_spend_df

def main_adv_spend(days_count=1):
    df = processed_adv_spend(days_count)
    df['updTime'] = df['updTime'].astype(str)
    table = safe_open_spreadsheet('victoria_project')
    sheet = table.worksheet('БД_Рекламные_затраты')
    send_df_to_google(df, sheet)