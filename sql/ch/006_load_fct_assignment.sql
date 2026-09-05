-- Новые назначения из ODS → DDS (append-only, идемпотентно).
-- Point-in-time через ASOF JOIN. Уже загруженные id — LEFT ANTI JOIN (по равенству).

INSERT INTO dds.fct_assignment
(
    assignment_id,
    equipment_sk,
    dispatcher_sk,
    equipment_id,
    dispatcher_id,
    started_at,
    ended_at,
    work_hours,
    equipment_type,
    shift_code
)
SELECT
    a.id AS assignment_id,
    e.equipment_sk,
    d.dispatcher_sk,
    a.equipment_id,
    a.dispatcher_id,
    a.started_at,
    a.ended_at,
    a.work_hours,
    e.equipment_type,
    d.shift_code
FROM
(
    SELECT
        s.id AS id,
        argMax(s.equipment_id, s._loaded_at) AS equipment_id,
        argMax(s.dispatcher_id, s._loaded_at) AS dispatcher_id,
        argMax(s.started_at, s._loaded_at) AS started_at,
        argMax(s.ended_at, s._loaded_at) AS ended_at,
        argMax(s.work_hours, s._loaded_at) AS work_hours
    FROM ods.equipment_assignments AS s
    GROUP BY s.id
) AS a
LEFT ANTI JOIN dds.fct_assignment AS already
    ON a.id = already.assignment_id
ASOF LEFT JOIN dds.dim_equipment AS e
    ON a.equipment_id = e.equipment_id AND a.started_at >= e.valid_from
ASOF LEFT JOIN dds.dim_dispatcher AS d
    ON a.dispatcher_id = d.dispatcher_id AND a.started_at >= d.valid_from
WHERE e.equipment_sk > 0
  AND d.dispatcher_sk > 0
