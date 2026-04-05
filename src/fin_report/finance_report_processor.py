import pandas as pd
from datetime import datetime


class FinRepProcessor:
    """Класс для обработки сырых рекламных данных"""
    def __init__(self):
        # Словарь-роутер для разных типов данных
        self._processors = {
            "_process_fin_rep": self._process_fin_rep
        }

    # === Обработчики для фин отчета ===
    def _process_fin_rep(self, data):
        # 1. Создаем пустой список для всех строк
        all_rows_list = []
        # 2. Перебираем результаты
        for chunk in data: 
            if isinstance(chunk, list):
                # Если чанк — это список строк
                all_rows_list.extend(chunk) 
            elif isinstance(chunk, dict):
                # Если вдруг чанк — это одна строка (одиночный словарь)
                all_rows_list.append(chunk)
            elif isinstance(chunk, Exception):
                print(f"⚠️ Ошибка в одном из потоков: {chunk}")

        # 3. Создаем единый DataFrame из гигантского списка словарей
        if not all_rows_list:
            print("⚠️ Данные для создания DataFrame отсутствуют!")
            return pd.DataFrame()
        df = pd.DataFrame(all_rows_list)
        df['updated_at'] = (datetime.now()).strftime("%Y-%m-%d %H:%M")
        df = df.fillna(0)
        print(f"✅ Создан единый DataFrame: {len(df)} строк")
        return df