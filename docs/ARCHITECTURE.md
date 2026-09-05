# Архитектура DWH

## Общий поток данных

```text
PostgreSQL (OLTP-источник)
        │
        │  ClickHouse SQL: postgresql() + INSERT
        ▼
      ODS  — сырой слепок (ReplacingMergeTree), почти копия полей
        │
        │  Airflow: dwh_pipeline / transform_dds
        ▼
      DDS  — история версий (SCD Type 2) + факт с суррогатными ключами
        │
        │  Airflow: dwh_pipeline / build_dm
        ▼
       DM  — денормализованные витрины под отчёты
        │
        ├──► MinIO (S3): cold tier ClickHouse (TTL TO VOLUME, без DELETE)
        └──► Apache Superset: дашборды по dm.* (localhost:8088)
```

Стек: PostgreSQL 16, ClickHouse, Apache Airflow, MinIO, Apache Superset — в Docker Compose.

---

## Слой ODS (Operational Data Store)

### Назначение

Сохранить данные из источника **как есть**, без трансформаций и истории.
Слой нужен, чтобы:

- отделить получение данных от их обработки;
- иметь «точку отката» — если DDS сломан, ODS не тронут;
- хранить всё в ClickHouse (быстрые аналитические запросы), а не читать из PG на лету.

### Почему не читать из PG напрямую

Engine `PostgreSQL` в ClickHouse создаёт «окно» в PG и читает данные при каждом запросе.
Это **не хранилище**: нет истории, нет изоляции от источника, нет контроля нагрузки на PG.
ODS = собственные таблицы `ReplacingMergeTree` с данными внутри ClickHouse.

### Способ загрузки PG → ODS

Рассмотренные варианты:

| Вариант | Суть | Почему не берём |
|---|---|---|
| `MaterializedPostgreSQL` | CDC-подобная репликация через слоты PG | experimental, требует `wal_level=logical`, капризна в Docker |
| Debezium / Kafka | полноценный CDC-поток | отдельный контур (Kafka, коннекторы), избыточно для проекта |
| Python extract в Airflow | `SELECT` из PG в Python, потом `INSERT` в CH | не нужен промежуточный Python; медленнее чем SQL внутри CH |
| **`postgresql()` + INSERT** | SQL со стороны ClickHouse: `INSERT INTO ods.* SELECT … FROM postgresql(...)` | **выбираем**: минимум кода, нет промежуточного слоя, CH сам читает PG |

**Итоговое решение:** `INSERT INTO ods.table SELECT … FROM postgresql('host', 'db', 'table', 'user', 'pass')`.
Airflow запускает этот SQL через HTTP-клиент / `clickhouse-client`.
Сам Airflow строки **не тащит** — только оркестрирует SQL внутри CH.

**CDC** — возможное развитие: при росте объёма и требовании к задержке < минуты.

### Стратегия загрузки (без TRUNCATE)

На **каждую** ODS-таблицу отдельно:

| Состояние | Действие |
|---|---|
| `count() = 0` | полный `INSERT` из PG (файлы `003a/b/c`) |
| `count() > 0` | инкремент: новые id / более свежий `updated_at` (`005a/b/c`) |

Повторный прогон по непустой таблице не копирует те же строки.  
При `docker compose up` сервис `airflow-trigger-pipeline` один раз делает `airflow dags trigger dwh_pipeline`.

### Движок ODS и дедупликация

Движок: `ReplacingMergeTree` (`ORDER BY id`, версия = `updated_at` / для факта `created_at`).

- Фоновый merge **когда-нибудь** оставит одну строку на `id` — момент **не гарантирован**.
- **`FINAL` не используем** в боевых запросах (на росте объёма дорого).
- **Алгоритм дедупа** — явный SQL при ODS → DDS: `GROUP BY id` + `argMax(колонка, updated_at)`.
  `ReplacingMergeTree` только помогает чистить диск, пайплайн на merge не опирается.

### Таблицы ODS

```
ods.dispatchers
    id              Int32
    first_name      String
    last_name       String
    phone           Nullable(String)
    shift_code      String
    is_active       UInt8          -- Bool → UInt8 в CH
    updated_at      DateTime('Europe/Moscow')
    _loaded_at      DateTime('Europe/Moscow')   -- когда строка попала в ODS

ods.equipment
    id              Int32
    inventory_number String
    name            String
    equipment_type  String
    status          String         -- 'in_service' | 'repair' | 'idle'
    dispatcher_id   Nullable(Int32)
    updated_at      DateTime('Europe/Moscow')
    _loaded_at      DateTime('Europe/Moscow')

ods.equipment_assignments
    id              Int64
    equipment_id    Int32
    dispatcher_id   Int32
    started_at      DateTime('Europe/Moscow')
    ended_at        Nullable(DateTime('Europe/Moscow'))
    work_hours      Nullable(Decimal(8,2))
    created_at      DateTime('Europe/Moscow')
    _loaded_at      DateTime('Europe/Moscow')
```

Движок: `ReplacingMergeTree` по бизнес-ключу (`id`) + версия (`updated_at` / `created_at`). Гарантию «одна строка на id» даём сами через `argMax` при чтении в DDS, не через `FINAL`.

---

## Слой DDS (Detail Data Store)

### Назначение

Хранить **историю изменений** (SCD Type 2) и связать факты с версией измерений на дату события.
Витрины считаются на истории, а не только на текущем слепке справочников.

### Алгоритм ODS → DDS (дедуп + effective_from / effective_to)

Имена в DDL: `valid_from` / `valid_to` (= `effective_from` / `effective_to`).

**1. Дедуп входа (dims)** — одна актуальная строка на бизнес-ключ:

```sql
SELECT
    id,
    argMax(first_name, updated_at) AS first_name,
    -- … остальные атрибуты через argMax(..., updated_at)
    max(updated_at) AS updated_at
FROM ods.dispatchers
GROUP BY id
```

То же для `equipment`. Для факта: ключ `assignment_id`; уже есть в DDS → не вставляем снова.

**2. SCD Type 2 для dims** (после дедупа), по каждому `dispatcher_id` / `equipment_id`:

Для изменившегося id **не** строим историю с нуля: берём версии из DDS, правим только хвост (текущую), добавляем строку из ODS → DELETE id → INSERT набора.

| Ситуация | Действие |
|---|---|
| Ключа ещё нет в DDS | INSERT одной версии: `valid_from = updated_at`, `valid_to = ∞` (`2105-12-31`), `is_current = 1`, новый SK |
| Есть текущая версия, атрибуты те же | ничего (идемпотентный прогон) |
| Есть текущая, атрибуты изменились | **delete + insert только этого id** (новый набор версий) |

Оба поля обязательны: `valid_from` + `valid_to`.  
Интервал полуоткрытый: `[valid_from, valid_to)`.  
`valid_*` из `updated_at` **источника**, не из времени ETL.

**Новый набор для изменившегося id:**

1. Читаем из DDS все версии этого id.
2. Собираем строки:
   - уже закрытые (`is_current = 0`) — **без изменений**;
   - бывшая текущая — те же атрибуты, `valid_to = updated_at` из ODS, `is_current = 0`;
   - новая — атрибуты из ODS, `valid_from = updated_at`, `valid_to = ∞` (`2105-12-31`), `is_current = 1`, **новый SK**.
3. `DELETE WHERE <business_id> IN (...)` → `INSERT` набора.

Неизменившиеся id не трогаем. SK: `max(sk) + 1` в прогоне.

**3. Факт** (`dds.fct_assignment`) — append-only.

Источник: `ods.equipment_assignments` (после дедупа по `id`).  
Уже лежащие в DDS `assignment_id` **не обновляем и не перечитываем** — только новые.

**Идемпотентность прогона**

```sql
… FROM (… ODS, GROUP BY id …) AS a
LEFT ANTI JOIN dds.fct_assignment AS already
  ON a.id = already.assignment_id
```

`LEFT ANTI JOIN` оставляет только те назначения, которых ещё нет в факте.  
Повторный trigger DAG не плодит дубли.

**Point-in-time: какой SK взять**

На дату/время `started_at` нужна **версия** dim, а не «текущая» строка справочника.

В SQL это `ASOF LEFT JOIN` (эквивалент: наибольший `valid_from ≤ started_at` при том же бизнес-ключе; интервал dim — `[valid_from, valid_to)`):

```sql
ASOF LEFT JOIN dds.dim_equipment  AS e
  ON a.equipment_id = e.equipment_id AND a.started_at >= e.valid_from
ASOF LEFT JOIN dds.dim_dispatcher AS d
  ON a.dispatcher_id = d.dispatcher_id AND a.started_at >= d.valid_from
WHERE e.equipment_sk > 0 AND d.dispatcher_sk > 0
```

В факт пишем уже готовые `equipment_sk`, `dispatcher_sk` и лёгкую денорм  
(`equipment_type`, `shift_code` с той же версии).  
Типовым отчётам / BI **не нужно** снова джойнить dim, чтобы узнать атрибуты «на момент работы».

**Почему не UPDATE факта при смене dim**

Факт фиксирует связь с версией **на момент события**. Если позже у техники сменился `status`, старые назначения по-прежнему смотрят на старый SK — история отчётов не «плывёт».

Реализация: таск Airflow `transform_dds` → `sql/ch/006_load_fct_assignment.sql`  
(dims до этого синхронизирует `dags/clickhouse_sql.py`).

### SCD Type 2 — почему, а не Type 1

- **Type 1** (перезаписать): быстро, но прошлое теряется.
  Если у диспетчера сменился телефон, все прошлые смены «увидят» новый телефон — витрина за прошлый квартал будет неверной.
- **Type 2** (добавить новую версию, закрыть старую): у каждой строки `valid_from` / `valid_to` / `is_current`.
  Витрина за любой месяц видит атрибуты **на дату факта**.

### dds.dim_dispatcher

```
dispatcher_sk       Int64        -- суррогатный ключ (новая версия = новый SK)
dispatcher_id       Int32        -- бизнес-ключ из PG
first_name          String
last_name           String
phone               Nullable(String)
shift_code          String
is_active           UInt8
valid_from          DateTime('Europe/Moscow')
valid_to            DateTime('Europe/Moscow')   -- 2105-12-31 если текущая
is_current          UInt8
```

Меняющиеся атрибуты: `phone`, `shift_code`, `is_active` — смена любого из них создаёт новую версию.

### dds.dim_equipment

```
equipment_sk        Int64        -- суррогатный ключ
equipment_id        Int32        -- бизнес-ключ из PG
inventory_number    String
name                String
equipment_type      String
status              String       -- меняется → причина SCD2
dispatcher_id       Nullable(Int32)
valid_from          DateTime('Europe/Moscow')
valid_to            DateTime('Europe/Moscow')
is_current          UInt8
```

Меняющийся атрибут: `status` (и переназначение `dispatcher_id`).

### dds.fct_assignment

Факт — **append-only**, новые строки только добавляются.

```
assignment_id       Int64        -- бизнес-ключ факта
equipment_sk        Int64        -- SK на версию техники, актуальную на started_at
dispatcher_sk       Int64        -- SK на версию диспетчера, актуальную на started_at
equipment_id        Int32        -- запасной бизнес-ключ
dispatcher_id       Int32
started_at          DateTime('Europe/Moscow')
ended_at            Nullable(DateTime('Europe/Moscow'))
work_hours          Nullable(Decimal(8,2))
equipment_type      String       -- денорм из dim: не джойнить ради типа
shift_code          String       -- денорм из dim диспетчера на дату
```

**Почему SK, а не только ID.**
Если бы мы джойнили по `dispatcher_id` к dim при запросе, мы бы взяли **текущую** версию, а не ту, которая была в момент смены. SK фиксирует нужную версию в момент загрузки факта.

**Лёгкая денорм в факте** (`equipment_type`, `shift_code`) — чтобы базовые срезы витрины не требовали JOIN с dims.

---

## Слой DM (Data Mart)

### Назначение

Три витрины = три вопроса от бизнеса. Аналитик делает `SELECT *` — JOIN не нужен.
Расчёт регулярный (`@daily`) из DDS в едином DAG `dwh_pipeline`, таск `build_dm`.
Полный пересчёт витрины: временная таблица → `INSERT` → **`EXCHANGE TABLES`** → `DROP` tmp (без TRUNCATE; атомарная подмена снимка).

«Топ техники / топ диспетчер за месяц» — **не отдельная витрина**, а отчёт поверх дневных метрик (`GROUP BY` + `ORDER BY … LIMIT`).

### 1. dm.equipment_utilization_daily — загрузка парка (факт работы)

**Вопрос:** какая техника реально отработала за день — сколько выездов и часов?

```
work_date           Date
equipment_id        Int32
inventory_number    String
equipment_name      String
equipment_type      String
dispatcher_id       Int32
dispatcher_first_name String
dispatcher_last_name  String
shift_code          String
assignments_cnt     UInt32
work_hours_sum      Decimal(10,2)
```

**Зерно:** день × единица техники.

Статус idle/repair **не** храним здесь (иначе путается с топом по работе) — это витрина `equipment_nonproductive_daily`.

### 2. dm.dispatcher_workload_daily — нагрузка на людей

**Вопрос:** кого из диспетчеров перегружаем, а кого недозагружаем?  
Часы и смены по диспетчеру за день (день/ночь).

```
work_date               Date
dispatcher_id           Int32
dispatcher_first_name   String
dispatcher_last_name    String
shift_code              String
assignments_cnt         UInt32
equipment_cnt           UInt32      -- уникальная техника за день
work_hours_sum          Decimal(10,2)
```

**Зерно:** день × диспетчер.

### 3. dm.equipment_nonproductive_daily — часы простоя/ремонта по машине

**Вопрос:** какая конкретная техника сколько часов не работала и почему — простой (`idle`) или ремонт (`repair`)?

```
work_date               Date
equipment_id            Int32
inventory_number        String
equipment_name          String
equipment_type          String
hours_idle              Decimal(10,2)   -- часов в idle за день
hours_repair            Decimal(10,2)   -- часов в repair за день
hours_not_in_service    Decimal(10,2)   -- idle + repair («не в работе»)
work_hours_fact         Decimal(10,2)   -- фактические часы работы из факта
```

**Зерно:** день × единица техники.

Считается из SCD2 `dds.dim_equipment` (пересечение версии с сутками → часы) + `dds.fct_assignment`.

**Почему денорм полная в DM.**  
В ClickHouse JOIN на больших объёмах дорог. Строка витрины самодостаточна для BI.

---

## MinIO (S3) — cold tier ClickHouse

Роль: **холодный диск** для старых parts MergeTree, не parquet-архив из DAG.

Свежие данные пишутся на локальный volume `hot`. На растущих таблицах:

- `PARTITION BY toYYYYMM(...)` — старые и новые месяцы в **разных** parts;
- TTL `date + INTERVAL 15 MONTH TO VOLUME 'cold'` — скользящее окно:
  старше 15 месяцев → `s3_cold` (MinIO `ch-cold`), свежее остаётся на hot.
  Без жёсткого обрезания календарного года 1 января. Без `DELETE`.

Переносим только растущие таблицы:

| Таблица | Partition / TTL поле | Почему cold |
|--------|----------------------|-------------|
| `ods.equipment_assignments` | `started_at` | линейный рост фактов |
| `dds.fct_assignment` | `started_at` | основной объём DDS |
| `dm.equipment_utilization_daily` | `work_date` | ежедневные агрегаты |
| `dm.dispatcher_workload_daily` | `work_date` | то же |
| `dm.equipment_nonproductive_daily` | `work_date` | то же |

Не переносим: `ods.dispatchers` / `ods.equipment`, `dds.dim_*` — маленькие
справочники, часто в JOIN.

Конфиг: `clickhouse/config.xml` (policy `tiered`). DDL cold-таблиц — в `002`/`004`
(`PARTITION BY` + TTL). `008_ttl_cold.sql` только `ALTER … MODIFY TTL` (без DROP);
перенос parts на S3 делает ClickHouse в фоне, не перезаливка из DAG.

Проверка (ожидаемо: партиции старше ~15 мес → `s3_cold`, свежие → `default`):

```sql
SELECT database, table, partition, disk_name, sum(rows) AS rows
FROM system.parts
WHERE active AND table IN (
  'equipment_assignments', 'fct_assignment',
  'equipment_utilization_daily', 'dispatcher_workload_daily',
  'equipment_nonproductive_daily'
)
GROUP BY database, table, partition, disk_name
ORDER BY database, table, partition;
```

---

## Денормализация — итоговый ответ

| Где | Что делаем | Почему |
|---|---|---|
| PG (источник) | нормализовано: FK, отдельные таблицы | источник не трогаем |
| ODS | почти как в PG, + `_loaded_at` | сырьё, без изменений |
| DDS (факт) | добавляем `equipment_type`, `shift_code` | лёгкий денорм чтобы не джойнить при каждом запросе |
| DM | полный денорм: ФИО, тип, статус, агрегаты в одной строке | BI читает без JOIN |

Денормализация **нарастает** от слоя к слою — это нормальная практика DWH.
В ODS денорма нет, потому что ODS — источник для дальнейших трансформаций, а не для отчётов.

---

## Apache Superset (BI)

UI: http://localhost:8088 (`admin` / `admin`). Отдельная БД метаданных `postgres-superset`.
Порт **8088**, чтобы не пересечься с Airflow (**8080**).

Superset ходит в ClickHouse `dm` (URI `clickhousedb://dwh:dwh@clickhouse:8123/dm`).
ODS/DDS в BI не отдаём — отчёты с витрин. Дашборд собирается в UI один раз;
скрины: `docs/screenshots/`. Подключение к `dm`: [SUPERSET.md](SUPERSET.md).

---

## Вне скоупа / развитие

| Возможность | Статус |
|---|---|
| CDC (Debezium, MaterializedPostgreSQL) | раздел «развитие» — следующий этап |
| SCD на всех таблицах | только dims dispatcher и equipment |
| Облачный деплой | архитектура cloud-ready (MinIO = S3, контейнеры), деплой — вне проекта |
| ERP, VictoriaMetrics | следующий этап развития проекта |
| Несколько витрин | **три** DM под бизнес-вопросы (см. слой DM); топы за месяц — SQL поверх них |
