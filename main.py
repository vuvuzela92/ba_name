import argparse
import asyncio
import inspect
import sys

from src.core.logging_utils import bind_context, setup_logging
from src.tasks_registry import TASKS


def main():
    setup_logging()

    parser = argparse.ArgumentParser(description="Task runner")
    parser.add_argument("task", choices=list(TASKS.keys()), help="Task name to run")
    parser.add_argument("--date_from", help="Start date in YYYY-MM-DD")
    parser.add_argument("--date_to", help="End date in YYYY-MM-DD")

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    task_data = TASKS[args.task]

    bind_context(task_name=args.task).info(task_data["desc"])

    try:
        original_func = task_data["original_func"]
        sig = inspect.signature(original_func)
        params = sig.parameters

        kwargs = {}
        if "date_from" in params and args.date_from:
            kwargs["date_from"] = args.date_from
        if "date_to" in params and args.date_to:
            kwargs["date_to"] = args.date_to

        if inspect.iscoroutinefunction(original_func):
            asyncio.run(original_func(**kwargs))
        else:
            original_func(**kwargs)

        bind_context(task_name=args.task).info("Task finished successfully")

    except Exception as exc:
        bind_context(task_name=args.task).exception(f"Task failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
