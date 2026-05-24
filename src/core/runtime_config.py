from dataclasses import dataclass, field
from pathlib import Path


@dataclass(slots=True)
class HttpSettings:
    request_timeout_sec: float = 30.0
    global_concurrency_limit: int = 16
    connector_limit: int = 100
    retry_max_attempts: int = 3
    retry_base_delay: float = 1.0
    retry_max_delay: float = 30.0
    retry_jitter_ratio: float = 0.2
    retry_statuses: set[int] = field(default_factory=lambda: {429, 500, 502, 503, 504})


@dataclass(slots=True)
class LoggingSettings:
    log_file: str = "logs/app.log"
    log_level: str = "INFO"
    log_rotation: str = "10 MB"
    log_retention: str = "24 hours"


@dataclass(slots=True)
class CredentialsSettings:
    tokens_path: str = "creds/tokens.json"
    google_creds_path: str = "creds/creds.json"


@dataclass(slots=True)
class RuntimeSettings:
    http: HttpSettings = field(default_factory=HttpSettings)
    logging: LoggingSettings = field(default_factory=LoggingSettings)
    credentials: CredentialsSettings = field(default_factory=CredentialsSettings)


def load_runtime_settings(project_root: Path | None = None) -> RuntimeSettings:
    _ = project_root
    return RuntimeSettings()
