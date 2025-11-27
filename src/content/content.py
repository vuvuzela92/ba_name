import requests
import pandas as pd
import gspread
from utils_content import load_api_tokens, get_content_data, safe_open_spreadsheet
from gspread_dataframe import set_with_dataframe


all_content_df = pd.DataFrame()

for account, api_token in load_api_tokens().items():
    df = get_content_data(account, api_token)
    all_content_df = pd.concat([all_content_df, df], ignore_index=True)

# Проверяем, является ли значение списком, прежде чем обращаться к индексу
all_content_df['photos'] = all_content_df['photos'].apply(lambda x: x[0]['tm'] if isinstance(x, list) and len(x) > 0 else None).astype(str)
all_content_df = all_content_df[['nmID', 'subjectName', 'vendorCode', 'photos', 'account']]

# Открывает доступ к гугл-таблице
table = safe_open_spreadsheet("victoria_project")

# Доступ к конкретному листу гугл таблицы
info_sheet = table.worksheet('БД_Фото')
set_with_dataframe(info_sheet, all_content_df)