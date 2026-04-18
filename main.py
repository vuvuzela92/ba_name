import argparse
import sys
import asyncio
import inspect
from src.tasks_registry import TASKS

def main():
    parser = argparse.ArgumentParser(description="Регулировщик запуска задач")
    
    parser.add_argument(
        "task",
        choices=list(TASKS.keys()), 
        help="Укажите задачу для запуска"
    )

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
        original_func = task_data["original_func"]
        
        # 🔍 Проверяем, какие аргументы принимает функция
        sig = inspect.signature(original_func)
        params = sig.parameters
        
        # Собираем только те аргументы, которые функция готова принять
        kwargs = {}
        if "date_from" in params and args.date_from:
            kwargs["date_from"] = args.date_from
        if "date_to" in params and args.date_to:
            kwargs["date_to"] = args.date_to

        # Запуск
        if inspect.iscoroutinefunction(original_func):
            asyncio.run(original_func(**kwargs))
        else:
            original_func(**kwargs)

        print(f"\n✅ Задача '{args.task}' успешно завершена.")
        
    except Exception as e:
        print(f"\n❌ Ошибка при выполнении задачи '{args.task}': {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()