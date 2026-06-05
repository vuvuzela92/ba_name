# импорт внутренних модулей
# Для работы с фин отчетами
from src.fin_report.finance_report_service import fetch_fin_reps_weekly
from src.fin_report.finance_report_processor import FinRepProcessor
# Для работы с гугл таблицами
from src.core.my_gspread import GoogleTabs
from src.core.config import google_tabs
from src.core.logging_utils import bind_context
# импорт внешних библиотек
import asyncio
import gspread
from datetime import datetime, timedelta

# Документы и бухгалтерия
def fin_rep_weekly(count_weeks=2):
    return asyncio.run(fin_rep_weekly_async(count_weeks))

async def fin_rep_weekly_async(count_weeks=2):
    """ Функция для получения обработанных результатов ежедневного финаносового отчете"""   
    log = bind_context(task_name="fin_rep_weekly", endpoint="prepare_google_sheet")
    data = await fetch_fin_reps_weekly(count_weeks)
    df = FinRepProcessor()._process_fin_rep(data)
    if df.empty:
        log.warning(
            "Financial report data is empty. Skip Google Sheets update. "
            "Possible reasons: expired WB API token, empty API response, or upstream request failures."
        )
        return
    # Переименуем на русский для удобной работе в гугл-таблице
    df_rus = df.copy()
    df_rus = df_rus.rename(columns={
        "realizationreport_id": "Номер отчёта",
        "date_from": "Дата начала отчётного периода",
        "date_to": "Дата конца отчётного периода",
        "create_dt": "Дата формирования отчёта",
        "currency_name": "Валюта отчёта",
        "suppliercontract_code": "Договор",
        "rrd_id": "Номер строки",
        "gi_id": "Номер поставки",
        "dlv_prc": "Фиксированный коэффициент склада по поставке",
        "fix_tariff_date_from": "Дата начала действия фиксации",
        "fix_tariff_date_to": "Дата конца действия фиксации",
        "subject_name": "Предмет",
        "nm_id": "Артикул WB",
        "brand_name": "Бренд",
        "sa_name": "Артикул продавца",
        "ts_name": "Размер",
        "barcode": "Баркод",
        "doc_type_name": "Тип документа",
        "quantity": "Количество",
        "retail_price": "Цена розничная",
        "retail_amount": "WB реализовал товар (продажа)",
        "sale_percent": "Согласованный дисконт, %",
        "commission_percent": "Размер кВВ, %",
        "office_name": "Склад",
        "supplier_oper_name": "Обоснование для оплаты",
        "order_dt": "Дата заказа",
        "sale_dt": "Дата продажи",
        "rr_dt": "Дата операции",
        "shk_id": "Штрихкод",
        "retail_price_withdisc_rub": "Цена розничная с учётом скидки",
        "delivery_amount": "Количество доставок",
        "return_amount": "Количество возвратов",
        "delivery_rub": "Услуги по доставке товара покупателю",
        "gi_box_type_name": "Тип коробов",
        "product_discount_for_report": "Итоговая согласованная скидка, %",
        "supplier_promo": "Промокод, %",
        "ppvz_spp_prc": "Скидка постоянного покупателя (СПП), %",
        "ppvz_kvw_prc_base": "Размер кВВ без НДС, % (базовый)",
        "ppvz_kvw_prc": "Итоговый кВВ без НДС, %",
        "sup_rating_prc_up": "Снижение кВВ из-за рейтинга, %",
        "is_kgvp_v2": "Снижение кВВ из-за акции, %",
        "ppvz_sales_commission": "Вознаграждение с продаж без НДС",
        "ppvz_for_pay": "К перечислению продавцу",
        "ppvz_reward": "Возмещение за выдачу и возврат на ПВЗ",
        "acquiring_fee": "Эквайринг / Комиссия за платежи",
        "acquiring_percent": "Комиссия за эквайринг, %",
        "payment_processing": "Тип платежа (эквайринг)",
        "acquiring_bank": "Банк-эквайер",
        "ppvz_vw": "Вознаграждение WB без НДС",
        "ppvz_vw_nds": "НДС с вознаграждения WB",
        "ppvz_office_name": "Офис доставки",
        "ppvz_office_id": "Номер офиса доставки",
        "ppvz_supplier_id": "Номер партнёра",
        "ppvz_supplier_name": "Партнёр",
        "ppvz_inn": "ИНН партнёра",
        "declaration_number": "Номер таможенной декларации",
        "bonus_type_name": "Вид логистики/штрафа/корректировки WB",
        "sticker_id": "ID стикера на товаре",
        "site_country": "Страна продажи",
        "srv_dbs": "Платная доставка (флаг)",
        "penalty": "Сумма штрафов",
        "additional_payment": "Корректировка вознаграждения WB",
        "rebill_logistic_cost": "Возмещение издержек по логистике",
        "rebill_logistic_org": "Организатор перевозки",
        "storage_fee": "Хранение",
        "deduction": "Удержания",
        "acceptance": "Платная приёмка",
        "assembly_id": "Номер сборочного задания",
        "kiz": "Код маркировки",
        "srid": "Уникальный ID заказа (srid)",
        "report_type": "Тип отчёта",
        "is_legal_entity": "Признак B2B-продажи",
        "trbx_id": "Номер короба платной приёмки",
        "installment_cofinancing_amount": "Скидка по программе софинансирования",
        "wibes_wb_discount_percent": "Скидка Wibes, %",
        "cashback_amount": "Удержание за начисленные баллы",
        "cashback_discount": "Компенсация скидки по кэшбэку",
        "cashback_commission_change": "Стоимость участия в кэшбэк-программе",
        "order_uid": "ID транзакции (корзины)"
    })


    # На случай если ВБ будет менять структуру отчетов, зададим порядок колонок
    cols_order = ['Номер отчёта',
    'Дата начала отчётного периода',
    'Дата конца отчётного периода',
    'Дата формирования отчёта',
    'Номер строки',
    'Номер поставки',
    'Предмет',
    'Артикул WB',
    'Бренд',
    'Артикул продавца',
    'Размер',
    'Баркод',
    'Тип документа',
    'Количество',
    'Цена розничная',
    'WB реализовал товар (продажа)',
    'Согласованный дисконт, %',
    'Размер кВВ, %',
    'Обоснование для оплаты',
    'Дата заказа',
    'Дата продажи',
    'Штрихкод',
    'Цена розничная с учётом скидки',
    'Количество доставок',
    'Количество возвратов',
    'Услуги по доставке товара покупателю',
    'Скидка постоянного покупателя (СПП), %',
    'Размер кВВ без НДС, % (базовый)',
    'Вознаграждение с продаж без НДС',
    'К перечислению продавцу',
    'Возмещение за выдачу и возврат на ПВЗ',
    'Эквайринг / Комиссия за платежи',
    'Комиссия за эквайринг, %',
    'Сумма штрафов',
    'Корректировка вознаграждения WB',
    'Возмещение издержек по логистике',
    'Хранение',
    'Удержания',
    'Платная приёмка',
    'Номер сборочного задания',
    'Уникальный ID заказа (srid)',
    'Тип отчёта',
    'Скидка Wibes, %',
    'account']
    missing_columns = [column for column in cols_order if column not in df_rus.columns]
    if missing_columns:
        log.error(
            "Financial report DataFrame is missing expected columns: {}. "
            "Skip Google Sheets update to avoid corrupt export.",
            missing_columns,
        )
        return
    # Только нужные колонки
    df_gs = df_rus[cols_order].copy()
    if df_gs.empty:
        log.warning("Financial report export DataFrame is empty after column selection. Skip Google Sheets update.")
        return
    # Создаем соединение с гугл-таблицей
    google_table = google_tabs.get("ba_name").get("title")
    table_sheet = google_tabs.get("ba_name").get("fin_rep_weekly")
    df_gs['upd_time'] = datetime.now().strftime('%d/%m/%Y, %H:%M:%S')
    # Создаем соединение с гугл-таблицей
    try:
        # Создаем соединение с гугл-таблицей
        google_connect = GoogleTabs(table_title=google_table, sheet_title=table_sheet)
        # Вставляем данные в гугл-таблицу
        google_connect._send_df_to_google(df_gs, google_connect.sheet_title)
        log.info("Financial report uploaded to Google Sheets rows={}", len(df_gs.index))
    except gspread.exceptions.SpreadsheetNotFound:
        log.error("Google spreadsheet not found: {}", google_table)
    except gspread.exceptions.WorksheetNotFound as e:
        log.error("Google worksheet not found: {} in spreadsheet {}", table_sheet, google_table)
    except StopIteration:
        log.error("Google worksheet lookup failed: {} in spreadsheet {}", table_sheet, google_table)
    except RuntimeError as e:
        log.error("Google Sheets connection error: {}", e)


