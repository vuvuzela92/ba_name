import pandas as pd
from utils_advert import get_all_adv_data, processed_adv_data, safe_open_spreadsheet, send_df_to_google
import asyncio
import numpy as np


if __name__ == "__main__":
    days_count = 1
    adv_data = asyncio.run(get_all_adv_data(days_count))
    adv_processed_data = processed_adv_data(adv_data)
    df = pd.DataFrame(adv_processed_data)
    # Добавляем данные о cpm рекламной кампании
    df['cpm'] = (df['sum'] / df['views'].replace(0, np.nan) * 1000).round(2)
    # Выбираем нужные для отображения в гугл-таблице колонки
    using_cols = ['date', 'avg_position', 'cr', 'atbs', 'article_id', 'advertId', 'views', 'clicks', 'sum', 'orders', 'sum_price', 'canceled', 'ctr', 'cpc', 'cpm', 'account']
    # Создаем из них датафрейм
    df_short = df[using_cols]
    df_short = df_short.fillna(0)
    df_short.loc[:, 'date'] = df_short['date'].astype(str)
    df_short.drop_duplicates
    table = safe_open_spreadsheet("victoria_project")
    sheet = table.worksheet("БД_Рекламная_статистика")
    send_df_to_google(df_short, sheet)