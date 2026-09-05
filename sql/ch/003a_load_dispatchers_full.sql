-- Полная загрузка: все строки из PG, которых ещё нет в ODS (идемпотентно при повторном прогоне).
INSERT INTO ods.dispatchers
(
    id, first_name, last_name, phone, shift_code, is_active, updated_at, _loaded_at
)
SELECT
    p.id, p.first_name, p.last_name, p.phone, p.shift_code,
    toUInt8(p.is_active),
    toDateTime(p.updated_at, 'Europe/Moscow'),
    now('Europe/Moscow')
FROM postgresql('postgres:5432', '{{POSTGRES_DB}}', 'dispatchers', '{{POSTGRES_USER}}', '{{POSTGRES_PASSWORD}}') AS p
LEFT ANTI JOIN ods.dispatchers AS o ON p.id = o.id;
