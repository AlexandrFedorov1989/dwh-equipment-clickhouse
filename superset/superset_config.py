"""Конфиг Superset для Docker Compose."""

import os


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value == "":
        raise RuntimeError(
            f"Не задана переменная окружения {name} (см. .env / docker-compose)."
        )
    return value


SECRET_KEY = _require_env("SUPERSET_SECRET_KEY")
SQLALCHEMY_DATABASE_URI = _require_env("DATABASE_URL")

# HTTP на localhost без HTTPS
TALISMAN_ENABLED = False
WTF_CSRF_ENABLED = True

FEATURE_FLAGS = {
    "ALERT_REPORTS": False,
}

CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}
DATA_CACHE_CONFIG = CACHE_CONFIG
FILTER_STATE_CACHE_CONFIG = CACHE_CONFIG
EXPLORE_FORM_DATA_CACHE_CONFIG = CACHE_CONFIG

ROW_LIMIT = 10000
SUPERSET_WEBSERVER_TIMEOUT = 120
