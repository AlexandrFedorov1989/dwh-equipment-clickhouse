-- Инкремент: новый id, более свежий updated_at, ИЛИ изменились атрибуты.
INSERT INTO ods.equipment
(
    id, inventory_number, name, equipment_type, status, dispatcher_id, updated_at, _loaded_at
)
SELECT
    p.id, p.inventory_number, p.name, p.equipment_type, p.status, p.dispatcher_id,
    toDateTime(p.updated_at, 'Europe/Moscow'),
    now('Europe/Moscow')
FROM postgresql('postgres:5432', '{{POSTGRES_DB}}', 'equipment', '{{POSTGRES_USER}}', '{{POSTGRES_PASSWORD}}') AS p
LEFT JOIN
(
    SELECT
        id,
        argMax(inventory_number, updated_at) AS inventory_number,
        argMax(name, updated_at) AS name,
        argMax(equipment_type, updated_at) AS equipment_type,
        argMax(status, updated_at) AS status,
        argMax(dispatcher_id, updated_at) AS dispatcher_id,
        max(updated_at) AS max_updated_at
    FROM ods.equipment
    GROUP BY id
) AS o ON p.id = o.id
WHERE o.id = 0
   OR toUnixTimestamp(toDateTime(p.updated_at, 'Europe/Moscow')) > toUnixTimestamp(o.max_updated_at)
   OR p.inventory_number != o.inventory_number
   OR p.name != o.name
   OR p.equipment_type != o.equipment_type
   OR p.status != o.status
   OR ifNull(p.dispatcher_id, 0) != ifNull(o.dispatcher_id, 0);
