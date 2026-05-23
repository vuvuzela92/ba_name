from pathlib import Path
import sys
from typing import Any

from loguru import logger


LOG_FORMAT = (
    "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | "
    "task={extra[task_name]} endpoint={extra[endpoint]} account={extra[account]} "
    "status={extra[status_code]} attempt={extra[attempt]} retries={extra[retries]} "
    "retry_sleep={extra[retry_sleep]} duration_ms={extra[duration_ms]} "
    "failed={extra[failed_tasks]} succeeded={extra[succeeded_tasks]} | {message}"
)


DEFAULT_CONTEXT: dict[str, Any] = {
    "task_name": "-",
    "endpoint": "-",
    "account": "-",
    "status_code": "-",
    "attempt": "-",
    "duration_ms": "-",
    "retries": "-",
    "retry_sleep": "-",
    "failed_tasks": "-",
    "succeeded_tasks": "-",
}


def bind_context(**kwargs: Any):
    context = DEFAULT_CONTEXT.copy()
    context.update({k: v for k, v in kwargs.items() if v is not None})
    return logger.bind(**context)


def setup_logging(log_file: str = "logs/app.log") -> None:
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    logger.remove()
    logger.configure(extra=DEFAULT_CONTEXT)

    logger.add(
        sys.stdout,
        level="INFO",
        format=LOG_FORMAT,
        colorize=False,
        enqueue=False,
    )
    logger.add(
        log_file,
        level="INFO",
        format=LOG_FORMAT,
        rotation="10 MB",
        retention="24 hours",
        encoding="utf-8",
        enqueue=False,
    )
