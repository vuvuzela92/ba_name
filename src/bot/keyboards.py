from aiogram.types import ReplyKeyboardMarkup, KeyboardButton


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Текущие цены"), KeyboardButton(text="Рекламная статистика")],
            [KeyboardButton(text="Финансовый отчет"), KeyboardButton(text="Топ товаров")],
            [KeyboardButton(text="Проблемные товары"), KeyboardButton(text="Сводка за день")],
            [KeyboardButton(text="Помощь")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие",
    )
