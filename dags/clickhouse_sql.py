"""HTTP-клиент ClickHouse для Airflow DAG'ов."""

from __future__ import annotations

import json
import logging
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value == "":
        raise RuntimeError(
            f"Не задана переменная окружения {name}. "
            "Скопируй .env.example → .env и задай значения (compose передаёт их в Airflow)."
        )
    return value


# Хост/порт — имена сервисов Compose (не секреты); учётки — только из env
CH_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123"))
CH_USER = _require_env("CLICKHOUSE_USER")
CH_PASSWORD = _require_env("CLICKHOUSE_PASSWORD")


def _sql_placeholders() -> dict[str, str]:
    """Плейсхолдеры в sql/ch/*.sql для table function postgresql(...)."""
    return {
        "{{POSTGRES_USER}}": _require_env("POSTGRES_USER"),
        "{{POSTGRES_PASSWORD}}": _require_env("POSTGRES_PASSWORD"),
        "{{POSTGRES_DB}}": _require_env("POSTGRES_DB"),
    }


def _render_sql(sql: str) -> str:
    """Подставить credentials из окружения в шаблоны {{POSTGRES_*}}."""
    rendered = sql
    for key, value in _sql_placeholders().items():
        rendered = rendered.replace(key, value)
    return rendered


def _sql_dir() -> Path:
    candidates = [
        Path(__file__).resolve().parent / "sql_ch",
        Path("/opt/airflow/dags/sql_ch"),
        Path("/opt/airflow/sql/ch"),
    ]
    for path in candidates:
        if path.is_dir():
            return path
    raise FileNotFoundError(
        "Каталог SQL не найден. Ожидали один из: "
        + ", ".join(str(p) for p in candidates)
        + ". Пересоздай Airflow: docker compose up -d --force-recreate airflow-scheduler airflow-webserver"
    )


def _base_url(**extra: str) -> str:
    params = {"user": CH_USER, "password": CH_PASSWORD, **extra}
    return f"http://{CH_HOST}:{CH_PORT}/?{urllib.parse.urlencode(params)}"


def _split_statements(sql: str) -> list[str]:
    without_line_comments = re.sub(r"--[^\n]*", "", sql)
    parts = []
    for chunk in without_line_comments.split(";"):
        stmt = chunk.strip()
        if stmt:
            parts.append(stmt)
    return parts


def run_query(sql: str, *, settings: dict[str, str] | None = None) -> str:
    """Один запрос; возвращает тело ответа (текст)."""
    url = _base_url(**(settings or {}))
    req = urllib.request.Request(url, data=sql.encode("utf-8"), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=600) as resp:
            return resp.read().decode("utf-8").strip()
    except urllib.error.HTTPError as e:
        # HTTPError — подкласс URLError; ловим первым, иначе теряем body
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"ClickHouse HTTP {e.code}: {body}\n--- SQL ---\n{sql[:2000]}"
        ) from e
    except urllib.error.URLError as e:
        raise RuntimeError(
            f"Не достучались до ClickHouse {url}: {e}. "
            "Проверь, что контейнер dwh-clickhouse healthy."
        ) from e


def query_rows(sql: str) -> list[dict[str, Any]]:
    """Выполнить SELECT и вернуть список строк как dict."""
    raw = run_query(sql.strip(), settings={"default_format": "JSONEachRow"})
    if not raw:
        return []
    return [json.loads(line) for line in raw.splitlines() if line.strip()]


def run_sql_file(filename: str) -> None:
    """Прочитать SQL-файл и выполнить все statements по очереди."""
    path = _sql_dir() / filename
    if not path.is_file():
        raise FileNotFoundError(
            f"SQL-файл не найден: {path} (содержимое каталога: {list(_sql_dir().iterdir())})"
        )
    sql = _render_sql(path.read_text(encoding="utf-8"))
    statements = _split_statements(sql)
    logger.info(
        "Выполняю %s: %s statement(s) → %s:%s",
        filename,
        len(statements),
        CH_HOST,
        CH_PORT,
    )
    for i, stmt in enumerate(statements, start=1):
        logger.info("  [%s/%s] %s…", i, len(statements), stmt[:80].replace("\n", " "))
        run_query(stmt)


# ─── ODS → DDS (SCD2) ───────────────────────────────────────────────────────

VALID_TO_OPEN = "2105-12-31 23:59:59"


def _sql_str(value: Any) -> str:
    # Экранирование значения для SQL-литерала / NULL
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{text}'"


def _norm_phone(value: Any) -> str | None:
    # Пустой телефон → NULL
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _norm_nullable_int(value: Any) -> int | None:
    # Опциональный int; пусто → None
    if value is None or value == "":
        return None
    return int(value)


def transform_dds() -> None:
    """Точка входа для Airflow task transform_dds."""
    sync_dim_dispatcher()
    sync_dim_equipment()
    run_sql_file("006_load_fct_assignment.sql")
    logger.info("ODS→DDS done")


def _table_count(table: str) -> int:
    return int(run_query(f"SELECT count() FROM {table}") or "0")


def load_ods_from_pg() -> None:
    """Без TRUNCATE. Пустая таблица → full; иначе → incremental."""
    d = _table_count("ods.dispatchers")
    e = _table_count("ods.equipment")
    a = _table_count("ods.equipment_assignments")
    logger.info("ODS before: dispatchers=%s equipment=%s assignments=%s", d, e, a)

    if d == 0:
        run_sql_file("003a_load_dispatchers_full.sql")
    else:
        run_sql_file("005a_load_dispatchers_incr.sql")

    if e == 0:
        run_sql_file("003b_load_equipment_full.sql")
    else:
        run_sql_file("005b_load_equipment_incr.sql")

    if a == 0:
        run_sql_file("003c_load_assignments_full.sql")
    else:
        run_sql_file("005c_load_assignments_incr.sql")

    logger.info(
        "ODS after: dispatchers=%s equipment=%s assignments=%s",
        _table_count("ods.dispatchers"),
        _table_count("ods.equipment"),
        _table_count("ods.equipment_assignments"),
    )


def task_load_ods() -> None:
    """Таск 1: DDL ODS + PG→ODS."""
    run_sql_file("002_ods_ddl.sql")
    load_ods_from_pg()
    logger.info("task load_ods done")


def task_transform_dds() -> None:
    """Таск 2: DDL DDS/DM + ODS→DDS + TTL cold (без DROP таблиц)."""
    run_sql_file("004_dds_dm_ddl.sql")
    transform_dds()
    run_sql_file("008_ttl_cold.sql")
    logger.info("task transform_dds done")


def task_build_dm() -> None:
    """Таск 3: DDS→DM (три витрины)."""
    build_dm()
    logger.info("task build_dm done")


def sync_dim_dispatcher() -> None:
    """SCD Type 2: синхронизация dds.dim_dispatcher из ODS."""
    ods_rows = query_rows(
        """
        SELECT
            s.id AS dispatcher_id,
            argMax(s.first_name, s.updated_at) AS first_name,
            argMax(s.last_name, s.updated_at) AS last_name,
            argMax(s.phone, s.updated_at) AS phone,
            argMax(s.shift_code, s.updated_at) AS shift_code,
            argMax(s.is_active, s.updated_at) AS is_active,
            max(s.updated_at) AS updated_at
        FROM ods.dispatchers AS s
        GROUP BY s.id
        """
    )
    current_rows = query_rows(
        """
        SELECT
            dispatcher_sk, dispatcher_id, first_name, last_name, phone,
            shift_code, is_active, valid_from, valid_to, is_current
        FROM dds.dim_dispatcher
        WHERE is_current = 1
        """
    )
    current_by_id = {int(r["dispatcher_id"]): r for r in current_rows}
    ods_by_id = {int(r["dispatcher_id"]): r for r in ods_rows}

    new_ids: list[int] = []
    changed_ids: list[int] = []
    for did, src in ods_by_id.items():
        cur = current_by_id.get(did)
        if cur is None:
            new_ids.append(did)
        elif _dispatcher_attrs_differ(cur, src):
            changed_ids.append(did)

    logger.info(
        "dim_dispatcher: new=%s changed=%s unchanged=%s",
        len(new_ids),
        len(changed_ids),
        len(ods_by_id) - len(new_ids) - len(changed_ids),
    )

    next_sk = _next_sk("dds.dim_dispatcher", "dispatcher_sk")
    inserts: list[str] = []

    for did in new_ids:
        src = ods_by_id[did]
        inserts.append(
            _dispatcher_values_row(next_sk, src, src["updated_at"], VALID_TO_OPEN, 1)
        )
        next_sk += 1

    for did in changed_ids:
        src = ods_by_id[did]
        versions = query_rows(
            f"""
            SELECT
                dispatcher_sk, dispatcher_id, first_name, last_name, phone,
                shift_code, is_active, valid_from, valid_to, is_current
            FROM dds.dim_dispatcher
            WHERE dispatcher_id = {did}
            ORDER BY valid_from, dispatcher_sk
            """
        )
        for ver in versions:
            if int(ver["is_current"]) == 1:
                inserts.append(
                    _dispatcher_values_row(
                        int(ver["dispatcher_sk"]),
                        ver,
                        ver["valid_from"],
                        src["updated_at"],
                        0,
                    )
                )
            else:
                inserts.append(
                    _dispatcher_values_row(
                        int(ver["dispatcher_sk"]),
                        ver,
                        ver["valid_from"],
                        ver["valid_to"],
                        0,
                    )
                )
        inserts.append(_dispatcher_values_row(next_sk, src, src["updated_at"], VALID_TO_OPEN, 1))
        next_sk += 1

    if changed_ids:
        ids_sql = ", ".join(str(i) for i in changed_ids)
        run_query(
            f"ALTER TABLE dds.dim_dispatcher DELETE WHERE dispatcher_id IN ({ids_sql})",
            settings={"mutations_sync": "1"},
        )

    if inserts:
        run_query(
            "INSERT INTO dds.dim_dispatcher "
            "(dispatcher_sk, dispatcher_id, first_name, last_name, phone, "
            "shift_code, is_active, valid_from, valid_to, is_current) VALUES "
            + ", ".join(inserts)
        )


def _dispatcher_attrs_differ(cur: dict[str, Any], src: dict[str, Any]) -> bool:
    return (
        str(cur["first_name"]) != str(src["first_name"])
        or str(cur["last_name"]) != str(src["last_name"])
        or _norm_phone(cur.get("phone")) != _norm_phone(src.get("phone"))
        or str(cur["shift_code"]) != str(src["shift_code"])
        or int(cur["is_active"]) != int(src["is_active"])
    )


def _dispatcher_values_row(
    sk: int,
    row: dict[str, Any],
    valid_from: Any,
    valid_to: Any,
    is_current: int,
) -> str:
    phone = _norm_phone(row.get("phone"))
    did = int(row["dispatcher_id"] if "dispatcher_id" in row else row["id"])
    return (
        "("
        f"{sk}, {did}, "
        f"{_sql_str(row['first_name'])}, {_sql_str(row['last_name'])}, {_sql_str(phone)}, "
        f"{_sql_str(row['shift_code'])}, {int(row['is_active'])}, "
        f"toDateTime({_sql_str(valid_from)}, 'Europe/Moscow'), "
        f"toDateTime({_sql_str(valid_to)}, 'Europe/Moscow'), "
        f"{is_current})"
    )


def sync_dim_equipment() -> None:
    """SCD Type 2: синхронизация dds.dim_equipment из ODS."""
    ods_rows = query_rows(
        """
        SELECT
            s.id AS equipment_id,
            argMax(s.inventory_number, s.updated_at) AS inventory_number,
            argMax(s.name, s.updated_at) AS name,
            argMax(s.equipment_type, s.updated_at) AS equipment_type,
            argMax(s.status, s.updated_at) AS status,
            argMax(s.dispatcher_id, s.updated_at) AS dispatcher_id,
            max(s.updated_at) AS updated_at
        FROM ods.equipment AS s
        GROUP BY s.id
        """
    )
    current_rows = query_rows(
        """
        SELECT
            equipment_sk, equipment_id, inventory_number, name, equipment_type,
            status, dispatcher_id, valid_from, valid_to, is_current
        FROM dds.dim_equipment
        WHERE is_current = 1
        """
    )
    current_by_id = {int(r["equipment_id"]): r for r in current_rows}
    ods_by_id = {int(r["equipment_id"]): r for r in ods_rows}

    new_ids: list[int] = []
    changed_ids: list[int] = []
    for eid, src in ods_by_id.items():
        cur = current_by_id.get(eid)
        if cur is None:
            new_ids.append(eid)
        elif _equipment_attrs_differ(cur, src):
            changed_ids.append(eid)

    logger.info(
        "dim_equipment: new=%s changed=%s unchanged=%s",
        len(new_ids),
        len(changed_ids),
        len(ods_by_id) - len(new_ids) - len(changed_ids),
    )

    next_sk = _next_sk("dds.dim_equipment", "equipment_sk")
    inserts: list[str] = []

    for eid in new_ids:
        src = ods_by_id[eid]
        inserts.append(
            _equipment_values_row(next_sk, src, src["updated_at"], VALID_TO_OPEN, 1)
        )
        next_sk += 1

    for eid in changed_ids:
        src = ods_by_id[eid]
        versions = query_rows(
            f"""
            SELECT
                equipment_sk, equipment_id, inventory_number, name, equipment_type,
                status, dispatcher_id, valid_from, valid_to, is_current
            FROM dds.dim_equipment
            WHERE equipment_id = {eid}
            ORDER BY valid_from, equipment_sk
            """
        )
        for ver in versions:
            if int(ver["is_current"]) == 1:
                inserts.append(
                    _equipment_values_row(
                        int(ver["equipment_sk"]),
                        ver,
                        ver["valid_from"],
                        src["updated_at"],
                        0,
                    )
                )
            else:
                inserts.append(
                    _equipment_values_row(
                        int(ver["equipment_sk"]),
                        ver,
                        ver["valid_from"],
                        ver["valid_to"],
                        0,
                    )
                )
        inserts.append(_equipment_values_row(next_sk, src, src["updated_at"], VALID_TO_OPEN, 1))
        next_sk += 1

    if changed_ids:
        ids_sql = ", ".join(str(i) for i in changed_ids)
        run_query(
            f"ALTER TABLE dds.dim_equipment DELETE WHERE equipment_id IN ({ids_sql})",
            settings={"mutations_sync": "1"},
        )

    if inserts:
        run_query(
            "INSERT INTO dds.dim_equipment "
            "(equipment_sk, equipment_id, inventory_number, name, equipment_type, "
            "status, dispatcher_id, valid_from, valid_to, is_current) VALUES "
            + ", ".join(inserts)
        )


def _equipment_attrs_differ(cur: dict[str, Any], src: dict[str, Any]) -> bool:
    return (
        str(cur["inventory_number"]) != str(src["inventory_number"])
        or str(cur["name"]) != str(src["name"])
        or str(cur["equipment_type"]) != str(src["equipment_type"])
        or str(cur["status"]) != str(src["status"])
        or _norm_nullable_int(cur.get("dispatcher_id")) != _norm_nullable_int(src.get("dispatcher_id"))
    )


def _equipment_values_row(
    sk: int,
    row: dict[str, Any],
    valid_from: Any,
    valid_to: Any,
    is_current: int,
) -> str:
    disp = _norm_nullable_int(row.get("dispatcher_id"))
    eid = int(row["equipment_id"] if "equipment_id" in row else row["id"])
    return (
        "("
        f"{sk}, {eid}, "
        f"{_sql_str(row['inventory_number'])}, {_sql_str(row['name'])}, "
        f"{_sql_str(row['equipment_type'])}, {_sql_str(row['status'])}, "
        f"{'NULL' if disp is None else disp}, "
        f"toDateTime({_sql_str(valid_from)}, 'Europe/Moscow'), "
        f"toDateTime({_sql_str(valid_to)}, 'Europe/Moscow'), "
        f"{is_current})"
    )


def _next_sk(table: str, column: str) -> int:
    raw = run_query(f"SELECT ifNull(max({column}), 0) FROM {table}")
    return int(raw or "0") + 1


# ─── DDS → DM (полный пересчёт через EXCHANGE) ───────────────────────────────

def build_dm() -> None:
    """Три витрины: tmp → INSERT → EXCHANGE → DROP tmp."""
    run_sql_file("007a_dm_equipment_utilization.sql")
    run_sql_file("007b_dm_dispatcher_workload.sql")
    run_sql_file("007c_dm_equipment_nonproductive.sql")
    logger.info("DDS→DM done")
