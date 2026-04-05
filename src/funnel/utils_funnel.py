import os
import aiohttp
import asyncio
from datetime import datetime, timedelta
import gspread
from time import time
from calendar import monthrange
import logging
from dotenv import load_dotenv
import json
import pandas as pd

# Импортируем переменные окружения
load_dotenv()

def load_api_tokens():
    # Получаем путь к текущему файлу (utils_advert.py или main.py)
    current_file = os.path.abspath(__file__)
    
    # Поднимаемся до корня проекта: 
    # .../ba_name/src/advert/ → .../ba_name/
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_file)))
    
    # Формируем путь к tokens.json в папке creds
    tokens_path = os.path.join(project_root, 'creds', 'tokens.json')
    
    if not os.path.isfile(tokens_path):
        print(f"Файл tokens.json не найден по пути: {tokens_path}")
        return None

    try:
        with open(tokens_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError:
        print(f"Ошибка декодирования JSON в файле: {tokens_path}")
        return None

def safe_open_spreadsheet(title, retries=5, delay=5):
    """
    Пытается открыть таблицу с повторными попытками при APIError 503.
    """
    # Поднимаемся на 2 уровня: funnel → src → ba_name
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    creds_path = os.path.join(project_root, 'creds', 'creds.json')
    gc = gspread.service_account(filename=creds_path)
    
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

async def get_funnel_v3(date_start: None, date_end: None, account: str, api_token: str):
    """Получение статистики по воронке продаж Wildberries"""
    products_list = []
    headers = {"Authorization": api_token}
    normal_delay = 2
    retry_delay = 20
    url = "https://seller-analytics-api.wildberries.ru/api/analytics/v3/sales-funnel/products"
    start = date_start
    end = date_end
    limit = 1000
    offset = 0
    max_attempts = 30
    attempt = 0
    semaphore = asyncio.Semaphore(10)
    
    async with semaphore:
        async with aiohttp.ClientSession(headers=headers) as session:
            while True:
                payload = {
                    "selectedPeriod": {
                        "start": start.strftime("%Y-%m-%d"),
                        "end": end.strftime("%Y-%m-%d")
                    },
                    "limit": limit,
                    "offset": offset
                }

                try:
                    async with session.post(url, json=payload) as res:
                        if res.status == 200:
                            data = await res.json()
                            products = data.get("data", {}).get("products", [])

                            if not products:
                                logging.info(f"📭 Нет данных для {account}")
                                break

                            for p in products:
                                p["account"] = account
                            products_list.extend(products)

                            logging.info(f"✅ Получено {len(products_list)} товаров ({len(products)} новых) для {account} за период {payload['selectedPeriod']}")

                            if len(products) < limit:
                                break

                            offset += len(products)
                            attempt = 0
                            await asyncio.sleep(normal_delay)

                        elif res.status == 429:
                            logging.info(f"⚠️ Ошибка 429 для {account}: слишком много запросов, ждем {retry_delay} сек.")
                            await asyncio.sleep(retry_delay)
                            retry_delay += 0.1
                            attempt += 1
                            if attempt >= max_attempts:
                                logging.info(f"🚫 Превышено число попыток ({max_attempts}) для {account}")
                                break
                            continue

                        elif res.status in (400, 401, 403):
                            err = await res.json()
                            logging.info(f"⚠️ Ошибка {res.status} для {account}: {err.get('detail', 'Ошибка доступа')}")
                            return None

                        else:
                            logging.info(f"⚠️ Неожиданный статус {res.status} для {account}")
                            attempt += 1
                            if attempt >= max_attempts:
                                break

                except aiohttp.ClientError as err:
                    logging.info(f"🌐 Сетевая ошибка: {err}")
                    attempt += 1
                    if attempt >= max_attempts:
                        break

                except Exception as e:
                    logging.info(f"💥 Неожиданная ошибка: {e}")
                    break

        if products_list:
            logging.info(f"🟢 Завершено получение данных по {account}. Всего товаров: {len(products_list)}")
            return products_list
        else:
            logging.info(f"❌ Не удалось получить данные по воронке продаж для {account}")
            return None

async def fetch_all(date_start: int, date_end: None):
    # Создаем задачник для получения данных о поставках по всем аккаунтам асинхронно
    tasks = [get_funnel_v3(date_start, date_end, account, api_token) for account, api_token in load_api_tokens().items()]
    res = await asyncio.gather(*tasks)
    return res

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

async def process_funnel_daily(days_count=1):
    """
    Оптимизированная версия: собираем ВСЕ данные в один DataFrame за 3 месяца
    """
    # === 1. УКАЗЫВАЕМ НУЖНЫЙ ПЕРИОД В  ===
    bath_size = 28
    date_ranges = []
    for day_num in range(1, days_count + 1):
        found_day = datetime.now()-timedelta(days=day_num)
        first_date, last_date = found_day, found_day
        date_ranges.append((first_date, last_date))
    
    print(f"📅 Запрашиваем данные за {days_count} дней...")

    batches = batchify(date_ranges, bath_size)

    # === 2. ПАРАЛЛЕЛЬНЫЙ ЗАПРОС ВСЕХ МЕСЯЦЕВ ===
    list_dfs = []
    for batch in batches:
        tasks = [fetch_all(first, last) for first, last in batch]
        results = await asyncio.gather(*tasks)
        
        print(f"✅ Получено {sum(len(r) for r in results)} записей")
        
        # === 3. ОБЪЕДИНЯЕМ ВСЕ ДАННЫЕ В ОДИН СПИСОК ===
        all_products = []
        for result in results:
            for acc_data in result:
                if acc_data:
                    all_products.extend(acc_data)
        
        print(f"📦 Обработано {len(all_products)} товаров")
        
        # === 4. ОБРАБОТКА ВСЕХ ДАННЫХ ===
        rows = []
        for product in all_products:
            # Извлекаем данные 
            prod_info = product.get("product", {})
            stat = product.get("statistic", {})
            selected = stat.get("selected", {})
            time_to_ready = selected.get("timeToReady", {})
            
            # Базовая информация
            row = {
                "account": product.get("account"),
                "nm_id": prod_info.get("nmId"),
                "vendor_code": prod_info.get("vendorCode"),  
                "title": prod_info.get("title"),
                "subject_id": prod_info.get("subjectId"),
                "subject_name": prod_info.get("subjectName"),
                "brand_name": prod_info.get("brandName"),
                "product_rating": prod_info.get("productRating"),
                "feedback_rating": prod_info.get("feedbackRating"),
                "stocks_wb": prod_info.get("stocks", {}).get("wb"),
                "stocks_mp": prod_info.get("stocks", {}).get("mp"),
                "balance_sum": prod_info.get("stocks", {}).get("balanceSum"),
            }
            
            # Метрики selected
            row.update({
                "open_count": selected.get("openCount"),
                "cart_count": selected.get("cartCount"),
                "order_count": selected.get("orderCount"),
                "orders_sum": selected.get("orderSum"),
                "buyout_count": selected.get("buyoutCount"),
                "buyout_sum": selected.get("buyoutSum"),
                "cancel_count": selected.get("cancelCount"),
                "cancel_sum": selected.get("cancelSum"),
                "avg_price": selected.get("avgPrice"),
                "avg_orders_count_per_day": selected.get("avgOrdersCountPerDay"),
                "share_order_percent": selected.get("shareOrderPercent"),
                "add_to_wish_list": selected.get("addToWishlist"),
                "time_to_ready": (
                    time_to_ready.get("days", 0) * 24 * 60 +
                    time_to_ready.get("hours", 0) * 60 +
                    time_to_ready.get("mins", 0)
                ),
                "localization_percent": selected.get("localizationPercent"),
                "date": selected.get("period", {}).get("end"),
            })
            
            rows.append(row)
                
        # === 5. ОДИН DataFrame ===
        df_full = pd.DataFrame(rows)
        list_dfs.append(df_full)
    df_final = pd.concat(list_dfs)
    # === 6. Создаем новые колонки ===
    # df_final['month'] = pd.to_datetime(df_final['date']).dt.strftime('%m-%Y')
    # df_final['wild'] = df_final['vendor_code'].str.extract(r'(wild\d+)')
    
    print(f"⚡ DataFrame создан: {len(df_final)} строк за {len(date_ranges)} дней")   
    return df_final

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

async def main_funnel_daily(days_count=30):
    df = await process_funnel_daily(days_count=days_count)
    df.drop_duplicates
    df = df.fillna(0)
    table = safe_open_spreadsheet("Наш Файл УУ ( Акселерация)")
    sheet = table.worksheet("БД_Воронка")
    send_df_to_google(df, sheet)