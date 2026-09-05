-- Слои DDS / DM (DDL). Пересчёт DM — через EXCHANGE в 007a/b/c.

-- ─── DDS: измерения SCD Type 2 ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dds.dim_dispatcher
(
    dispatcher_sk   Int64,
    dispatcher_id   Int32,
    first_name      String,
    last_name       String,
    phone           Nullable(String),
    shift_code      String,
    is_active       UInt8,
    valid_from      DateTime('Europe/Moscow'),
    valid_to        DateTime('Europe/Moscow'),
    is_current      UInt8
)
ENGINE = MergeTree
ORDER BY (dispatcher_id, valid_from);

CREATE TABLE IF NOT EXISTS dds.dim_equipment
(
    equipment_sk      Int64,
    equipment_id      Int32,
    inventory_number  String,
    name              String,
    equipment_type    String,
    status            String,
    dispatcher_id     Nullable(Int32),
    valid_from        DateTime('Europe/Moscow'),
    valid_to          DateTime('Europe/Moscow'),
    is_current        UInt8
)
ENGINE = MergeTree
ORDER BY (equipment_id, valid_from);

-- ─── DDS: факт (append-only) ───────────────────────────────────────────────

-- PARTITION BY месяц; TTL скользящие 15 мес → cold (без жёсткого стыка 1 января).
CREATE TABLE IF NOT EXISTS dds.fct_assignment
(
    assignment_id   Int64,
    equipment_sk    Int64,
    dispatcher_sk   Int64,
    equipment_id    Int32,
    dispatcher_id   Int32,
    started_at      DateTime('Europe/Moscow'),
    ended_at        Nullable(DateTime('Europe/Moscow')),
    work_hours      Nullable(Decimal(8, 2)),
    equipment_type  String,
    shift_code      String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(started_at)
ORDER BY (started_at, equipment_id)
TTL started_at + INTERVAL 15 MONTH TO VOLUME 'cold'
SETTINGS storage_policy = 'tiered';

-- ─── DM: витрины ───────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dm.equipment_utilization_daily
(
    work_date               Date,
    equipment_id            Int32,
    inventory_number        String,
    equipment_name          String,
    equipment_type          String,
    dispatcher_id           Int32,
    dispatcher_first_name   String,
    dispatcher_last_name    String,
    shift_code              String,
    assignments_cnt         UInt32,
    work_hours_sum          Decimal(10, 2)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(work_date)
ORDER BY (work_date, equipment_id)
TTL work_date + INTERVAL 15 MONTH TO VOLUME 'cold'
SETTINGS storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS dm.dispatcher_workload_daily
(
    work_date               Date,
    dispatcher_id           Int32,
    dispatcher_first_name   String,
    dispatcher_last_name    String,
    shift_code              String,
    assignments_cnt         UInt32,
    equipment_cnt           UInt32,
    work_hours_sum          Decimal(10, 2)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(work_date)
ORDER BY (work_date, dispatcher_id)
TTL work_date + INTERVAL 15 MONTH TO VOLUME 'cold'
SETTINGS storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS dm.equipment_nonproductive_daily
(
    work_date               Date,
    equipment_id            Int32,
    inventory_number        String,
    equipment_name          String,
    equipment_type          String,
    hours_idle              Decimal(10, 2),
    hours_repair            Decimal(10, 2),
    hours_not_in_service    Decimal(10, 2),
    work_hours_fact         Decimal(10, 2)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(work_date)
ORDER BY (work_date, equipment_id)
TTL work_date + INTERVAL 15 MONTH TO VOLUME 'cold'
SETTINGS storage_policy = 'tiered';
