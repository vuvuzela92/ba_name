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

def get_content_data(account, api_token):
    url = 'https://content-api.wildberries.ru/content/v2/get/cards/list'
    headers = {
        "Authorization": api_token
    }
    
    payload = {
        "settings": {
            "cursor": {
                "limit": 100
            },
            "filter": {
                "withPhoto": -1
            }
        }
    }
    
    try:
        res = requests.post(url, json=payload, headers=headers)
        res.raise_for_status()
        result = res.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for account {account}: {e}")
        return pd.DataFrame()  # Возвращаем пустой DataFrame в случае ошибки
    
    content_df = pd.DataFrame(result['cards'])
    content_df['account'] = account
    
    while len(result['cards']) == payload['settings']['cursor']['limit']:
        updatedAt = result['cursor']['updatedAt']
        nmID = result['cursor']['nmID']
        
        payload['settings']['cursor']['updatedAt'] = updatedAt
        payload['settings']['cursor']['nmID'] = nmID
        
        try:
            res = requests.post(url, json=payload, headers=headers)
            res.raise_for_status()
            result = res.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching paginated data for account {account}: {e}")
            break
        
        new_data = pd.DataFrame(result['cards'])
        new_data['account'] = account
        content_df = pd.concat([content_df, new_data], ignore_index=True)
        
    
    return content_df