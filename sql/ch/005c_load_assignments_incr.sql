-- Инкремент факта: только новые id (append-only).
INSERT INTO ods.equipment_assignments
(
    id, equipment_id, dispatcher_id, started_at, ended_at, work_hours, created_at, _loaded_at
)
SELECT
    p.id, p.equipment_id, p.dispatcher_id,
    toDateTime(p.started_at, 'Europe/Moscow'),
    if(isNull(p.ended_at), NULL, toDateTime(p.ended_at, 'Europe/Moscow')),
    p.work_hours,
    toDateTime(p.created_at, 'Europe/Moscow'),
    now('Europe/Moscow')
FROM postgresql('postgres:5432', '{{POSTGRES_DB}}', 'equipment_assignments', '{{POSTGRES_USER}}', '{{POSTGRES_PASSWORD}}') AS p
LEFT ANTI JOIN ods.equipment_assignments AS o ON p.id = o.id;
