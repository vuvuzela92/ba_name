import pandas as pd
from src.core.config import google_tabs
from src.core.my_gspread import GoogleTabs


def process_prices(prices_data: list)->pd.DataFrame:
    # 1. Создаем пустой список для хранения всех строк
    all_rows = []

    for item in prices_data:
        # 2. Извлекаем данные
        nm_id = item.get('nmID')
        vendor_code = item.get('vendorCode')
        
        # достаем цену из первого размера
        sizes = item.get('sizes', [])
        if sizes:
            price = sizes[0].get('price')
            discounted_price = sizes[0].get('discountedPrice')
        else:
            price = None
            discounted_price = None

        # 3. Создаем словарь для каждой итерации
        row = {
            'nm_id': nm_id,
            'vendor_code': vendor_code,
            'price': price,
            'discounted_price': discounted_price
        }
        
        # 4. Добавляем этот словарь в наш список
        all_rows.append(row)

    # 5. Создаем DataFrame из списка словарей
    try:
        df = pd.DataFrame(all_rows)
    except Exception as e:
        print(f"Ошибка при создании датафрейма {e}")
        return pd.DataFrame()
    return df

def get_nm_ids():
    table = google_tabs.get("ba_name").get("title")
    sheet = google_tabs.get("ba_name").get("unit")
    google_tab = GoogleTabs(table, sheet)
    sheet_data = google_tab.sheet_title.get_all_values()
    #  Создаем датафрейм
    df = pd.DataFrame(sheet_data[2:], # Данные начиная с 3-й строки
                    columns=sheet_data[1]) # Названия колонок с 2-й строки
    nm_ids = df['SKU'].apply(lambda x: int(x)).to_list()
    return nm_ids