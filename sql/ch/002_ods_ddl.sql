-- Слои + ODS-таблицы (можно запускать одним файлом после 001 или вместо него)

CREATE DATABASE IF NOT EXISTS ods;
CREATE DATABASE IF NOT EXISTS dds;
CREATE DATABASE IF NOT EXISTS dm;

-- ODS: сырой слепок источника PostgreSQL (поля 1:1 + _loaded_at)

CREATE TABLE IF NOT EXISTS ods.dispatchers
(
    id          Int32,
    first_name  String,
    last_name   String,
    phone       Nullable(String),
    shift_code  String,
    is_active   UInt8,
    updated_at  DateTime('Europe/Moscow'),
    _loaded_at  DateTime('Europe/Moscow') DEFAULT now('Europe/Moscow')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id;

CREATE TABLE IF NOT EXISTS ods.equipment
(
    id                Int32,
    inventory_number  String,
    name              String,
    equipment_type    String,
    status            String,
    dispatcher_id     Nullable(Int32),
    updated_at        DateTime('Europe/Moscow'),
    _loaded_at        DateTime('Europe/Moscow') DEFAULT now('Europe/Moscow')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id;

-- Растущая таблица: PARTITION BY месяц + TTL скользящие 15 мес → cold (см. 008).
CREATE TABLE IF NOT EXISTS ods.equipment_assignments
(
    id              Int64,
    equipment_id    Int32,
    dispatcher_id   Int32,
    started_at      DateTime('Europe/Moscow'),
    ended_at        Nullable(DateTime('Europe/Moscow')),
    work_hours      Nullable(Decimal(8, 2)),
    created_at      DateTime('Europe/Moscow'),
    _loaded_at      DateTime('Europe/Moscow') DEFAULT now('Europe/Moscow')
)
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(started_at)
ORDER BY id
TTL started_at + INTERVAL 15 MONTH TO VOLUME 'cold'
SETTINGS storage_policy = 'tiered';
