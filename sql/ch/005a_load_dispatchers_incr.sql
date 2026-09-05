-- Инкремент: новый id, более свежий updated_at, ИЛИ изменились атрибуты.
-- (только updated_at недостаточно: TZ/precision у postgresql() иногда не даёт `>`.)
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
LEFT JOIN
(
    SELECT
        id,
        argMax(first_name, updated_at) AS first_name,
        argMax(last_name, updated_at) AS last_name,
        argMax(phone, updated_at) AS phone,
        argMax(shift_code, updated_at) AS shift_code,
        argMax(is_active, updated_at) AS is_active,
        max(updated_at) AS max_updated_at
    FROM ods.dispatchers
    GROUP BY id
) AS o ON p.id = o.id
WHERE o.id = 0
   OR toUnixTimestamp(toDateTime(p.updated_at, 'Europe/Moscow')) > toUnixTimestamp(o.max_updated_at)
   OR p.first_name != o.first_name
   OR p.last_name != o.last_name
   OR ifNull(p.phone, '') != ifNull(o.phone, '')
   OR p.shift_code != o.shift_code
   OR toUInt8(p.is_active) != o.is_active;
