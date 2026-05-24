"""CLI-точка входа для запуска Telegram-бота в режиме polling."""

from src.bot.app import run_polling


if __name__ == "__main__":
    run_polling()
