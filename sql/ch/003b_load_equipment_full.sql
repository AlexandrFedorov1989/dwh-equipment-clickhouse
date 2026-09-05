-- Полная загрузка: все строки из PG, которых ещё нет в ODS (идемпотентно при повторном прогоне).
INSERT INTO ods.equipment
(
    id, inventory_number, name, equipment_type, status, dispatcher_id, updated_at, _loaded_at
)
SELECT
    p.id, p.inventory_number, p.name, p.equipment_type, p.status, p.dispatcher_id,
    toDateTime(p.updated_at, 'Europe/Moscow'),
    now('Europe/Moscow')
FROM postgresql('postgres:5432', '{{POSTGRES_DB}}', 'equipment', '{{POSTGRES_USER}}', '{{POSTGRES_PASSWORD}}') AS p
LEFT ANTI JOIN ods.equipment AS o ON p.id = o.id;
