# Apache Superset — подключение к DM

UI: http://localhost:8088  
Логин/пароль UI — `SUPERSET_ADMIN_USER` / `SUPERSET_ADMIN_PASSWORD` из `.env`.  
Порт **8088** (Airflow — **8080**).

Порядок: стек поднят → успешен DAG `dwh_pipeline` → в `dm.*` есть данные → Superset.

## Подключить ClickHouse

1. **Settings → Database connections → + Database** → ClickHouse  
2. Внизу: **Connect this database with a SQLAlchemy URI string instead**  
3. URI — подставьте значения из `.env` (`CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`):

```
clickhousedb://<CLICKHOUSE_USER>:<CLICKHOUSE_PASSWORD>@clickhouse:8123/dm
```

4. **DISPLAY NAME:** `ClickHouse DM`  
5. **Test connection** → **Connect**

Хост **`clickhouse`**, не `localhost`. Читаем только схему **`dm`** (три витрины).

## Датасеты

| Имя | Таблица |
|-----|--------|
| Загрузка техники | `equipment_utilization_daily` |
| Нагрузка диспетчеров | `dispatcher_workload_daily` |
| Простой техники | `equipment_nonproductive_daily` |

Скрины дашбордов: `docs/screenshots/`.
