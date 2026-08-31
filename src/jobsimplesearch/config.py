from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

SMTP_SECURITY_VALUES = frozenset({"ssl", "starttls", "none"})


def _resolve_project_root() -> Path:
    configured = os.environ.get("JOBSEARCH_PROJECT_ROOT")
    if configured:
        return Path(configured).expanduser().resolve()
    source_checkout = Path(__file__).resolve().parents[2]
    for candidate in (Path.cwd().resolve(), source_checkout):
        if (candidate / "migrations").is_dir() and (
            (candidate / "pyproject.toml").exists() or (candidate / "jobs.db").exists()
        ):
            return candidate
    return source_checkout


PROJECT_ROOT = _resolve_project_root()
ENV_PATH = PROJECT_ROOT / ".env"


def _bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().casefold() in {"1", "true", "yes", "on"}


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


def _float(name: str, default: float) -> float:
    return float(os.environ.get(name, str(default)))


def _path(name: str, default: Path) -> Path:
    value = os.environ.get(name)
    path = Path(value).expanduser() if value else default
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


def _smtp_security() -> str:
    raw = os.environ.get("SMTP_SECURITY")
    if raw is None:
        return "starttls" if _bool("SMTP_USE_TLS", True) else "none"
    value = raw.strip().casefold()
    if value not in SMTP_SECURITY_VALUES:
        supported = ", ".join(sorted(SMTP_SECURITY_VALUES))
        raise ValueError(f"SMTP_SECURITY must be one of: {supported}; got {raw!r}")
    return value


def _public_base_url() -> str | None:
    value = os.environ.get("PUBLIC_BASE_URL", "").strip().rstrip("/")
    return value or None


@dataclass(frozen=True)
class AISettings:
    api_key: str | None
    model: str
    thinking_level: str
    temperature: float
    top_p: float
    top_k: int
    target_rpm: int
    max_concurrency: int
    provider_tpm: int
    tpm_safety_factor: float
    chars_per_token_estimate: float
    max_jobs_per_run: int
    max_attempts_per_job: int
    max_schema_repair_attempts: int
    retry_base_seconds: float
    retry_max_seconds: float
    probe_mode: bool


@dataclass(frozen=True)
class SearchSettings:
    location: str
    country: str
    hours_old: int
    results_wanted: int
    fetch_descriptions: bool
    delay_min_seconds: float
    delay_max_seconds: float
    details_max_jobs_per_run: int
    details_max_attempts_per_job: int
    details_delay_min_seconds: float
    details_delay_max_seconds: float
    details_stop_after_empty: int


@dataclass(frozen=True)
class EmailSettings:
    enabled: bool
    smtp_host: str
    smtp_port: int
    username: str | None
    password: str | None
    sender: str | None
    recipients: tuple[str, ...]
    smtp_security: str


@dataclass(frozen=True)
class SchedulerSettings:
    enabled: bool
    search_hour: int
    search_minute: int
    reminder_hour: int
    reminder_minute: int


@dataclass(frozen=True)
class WebSettings:
    host: str
    port: int
    public_base_url: str | None


@dataclass(frozen=True)
class WindowsAutomationSettings:
    enabled: bool
    pipeline_task_name: str
    portal_stop_task_name: str
    portal_shutdown_delay_seconds: int


@dataclass(frozen=True)
class Settings:
    project_root: Path
    database_path: Path
    timezone_name: str
    ai: AISettings
    search: SearchSettings
    email: EmailSettings
    scheduler: SchedulerSettings
    web: WebSettings
    windows_automation: WindowsAutomationSettings

    @property
    def timezone(self) -> ZoneInfo:
        return ZoneInfo(self.timezone_name)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Load environment-backed application settings once per process."""
    load_dotenv(ENV_PATH, override=False)

    legacy_database = PROJECT_ROOT / "jobs.db"
    default_database = (
        legacy_database
        if legacy_database.exists()
        else PROJECT_ROOT / "data" / "jobs.db"
    )
    timezone_name = os.environ.get("JOBSEARCH_TIMEZONE", "Europe/Amsterdam")
    thinking_level = os.environ.get("GEMMA_THINKING_LEVEL", "HIGH").strip().upper()
    if thinking_level not in {"HIGH", "MINIMAL"}:
        raise ValueError("GEMMA_THINKING_LEVEL must be HIGH or MINIMAL")
    ai_retry_base_seconds = _float("AI_RETRY_BASE_SECONDS", 2.0)
    ai_retry_max_seconds = _float("AI_RETRY_MAX_SECONDS", 30.0)
    if ai_retry_base_seconds < 0:
        raise ValueError("AI_RETRY_BASE_SECONDS must be greater than or equal to 0")
    if ai_retry_max_seconds < ai_retry_base_seconds:
        raise ValueError(
            "AI_RETRY_MAX_SECONDS must be greater than or equal to "
            "AI_RETRY_BASE_SECONDS"
        )

    recipients = tuple(
        item.strip()
        for item in os.environ.get("EMAIL_TO", "").split(",")
        if item.strip()
    )

    return Settings(
        project_root=PROJECT_ROOT,
        database_path=_path("JOBSEARCH_DB_PATH", default_database),
        timezone_name=timezone_name,
        ai=AISettings(
            api_key=os.environ.get("GEMINI_API_KEY") or None,
            model=os.environ.get("JOB_AI_MODEL", "gemma-4-31b-it"),
            thinking_level=thinking_level,
            temperature=_float("GEMMA_TEMPERATURE", 1.0),
            top_p=_float("GEMMA_TOP_P", 0.95),
            top_k=_int("GEMMA_TOP_K", 64),
            target_rpm=_int("AI_TARGET_RPM", 25),
            max_concurrency=_int("AI_MAX_CONCURRENCY", 25),
            provider_tpm=_int("AI_PROVIDER_TPM", 16000),
            tpm_safety_factor=_float("AI_TPM_SAFETY_FACTOR", 0.90),
            chars_per_token_estimate=_float("AI_CHARS_PER_TOKEN_ESTIMATE", 3.5),
            max_jobs_per_run=_int("AI_MAX_JOBS_PER_RUN", 0),
            max_attempts_per_job=_int("AI_MAX_ATTEMPTS_PER_JOB", 3),
            max_schema_repair_attempts=_int("AI_MAX_SCHEMA_REPAIR_ATTEMPTS", 1),
            retry_base_seconds=ai_retry_base_seconds,
            retry_max_seconds=ai_retry_max_seconds,
            probe_mode=_bool("AI_PROBE_MODE"),
        ),
        search=SearchSettings(
            location=os.environ.get("SEARCH_LOCATION", "Netherlands"),
            country=os.environ.get("SEARCH_COUNTRY", "Netherlands"),
            hours_old=_int("SEARCH_HOURS_OLD", 24),
            results_wanted=_int("SEARCH_RESULTS_WANTED", 1000),
            fetch_descriptions=_bool("SEARCH_FETCH_DESCRIPTIONS"),
            delay_min_seconds=_float("SEARCH_DELAY_MIN_SECONDS", 15),
            delay_max_seconds=_float("SEARCH_DELAY_MAX_SECONDS", 30),
            details_max_jobs_per_run=_int("DETAILS_MAX_JOBS_PER_RUN", 30),
            details_max_attempts_per_job=_int("DETAILS_MAX_ATTEMPTS_PER_JOB", 3),
            details_delay_min_seconds=_float("DETAILS_DELAY_MIN_SECONDS", 4),
            details_delay_max_seconds=_float("DETAILS_DELAY_MAX_SECONDS", 8),
            details_stop_after_empty=_int("DETAILS_STOP_AFTER_EMPTY", 3),
        ),
        email=EmailSettings(
            enabled=_bool("EMAIL_ENABLED"),
            smtp_host=os.environ.get("SMTP_HOST", ""),
            smtp_port=_int("SMTP_PORT", 587),
            username=os.environ.get("SMTP_USERNAME") or None,
            password=os.environ.get("SMTP_PASSWORD") or None,
            sender=os.environ.get("EMAIL_FROM") or None,
            recipients=recipients,
            smtp_security=_smtp_security(),
        ),
        scheduler=SchedulerSettings(
            enabled=_bool("SCHEDULER_ENABLED", True),
            search_hour=_int("SCHEDULER_SEARCH_HOUR", 0),
            search_minute=_int("SCHEDULER_SEARCH_MINUTE", 0),
            reminder_hour=_int("SCHEDULER_REMINDER_HOUR", 8),
            reminder_minute=_int("SCHEDULER_REMINDER_MINUTE", 0),
        ),
        web=WebSettings(
            host=os.environ.get("WEB_HOST", "0.0.0.0"),
            port=_int("WEB_PORT", 5000),
            public_base_url=_public_base_url(),
        ),
        windows_automation=WindowsAutomationSettings(
            enabled=_bool("WINDOWS_TASK_AUTOMATION"),
            pipeline_task_name=os.environ.get(
                "WINDOWS_PIPELINE_TASK_NAME",
                "JobSimpleSearch-Continue",
            ).strip(),
            portal_stop_task_name=os.environ.get(
                "WINDOWS_PORTAL_STOP_TASK_NAME",
                "JobSimpleSearch-ClosePortal",
            ).strip(),
            portal_shutdown_delay_seconds=max(
                _int("WINDOWS_PORTAL_SHUTDOWN_DELAY_SECONDS", 3),
                0,
            ),
        ),
    )


def reset_settings_cache() -> None:
    """Test helper for environment overrides."""
    get_settings.cache_clear()
