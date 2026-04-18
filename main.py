# main.py
import argparse
import sys
from src.tasks_registry import TASKS

def main():
    parser = argparse.ArgumentParser(description="Регулировщик запуска задач")
    
    # Обязательный аргумент: имя задачи
    parser.add_argument(
        "task",
        choices=list(TASKS.keys()), 
        help="Укажите задачу для запуска"
    )

    # Дополнительные аргументы для дат
    parser.add_argument("--date_from", help="Дата начала в формате YYYY-MM-DD")
    parser.add_argument("--date_to", help="Дата окончания в формате YYYY-MM-DD")
    
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    task_data = TASKS[args.task]
    
    print(f"\n{'='*50}")
    print(task_data["desc"])
    print(f"{'='*50}\n")
    
    try:
        # Извлекаем оригинальную функцию из обертки smart_run, 
        # чтобы передать аргументы напрямую
        original_func = task_data["original_func"]
        
        import asyncio
        import inspect

        # Если функция асинхронная, запускаем через asyncio.run
        if inspect.iscoroutinefunction(original_func):
            asyncio.run(original_func(date_from=args.date_from, date_to=args.date_to))
        else:
            original_func(date_from=args.date_from, date_to=args.date_to)

        print(f"\n✅ Задача '{args.task}' успешно завершена.")
    except Exception as e:
        print(f"\n❌ Ошибка при выполнении задачи '{args.task}': {e}")
        import traceback
        traceback.print_exc() # Поможет увидеть, где именно упал код
        sys.exit(1)

if __name__ == "__main__":
    main()