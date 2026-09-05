-- DM: непроизводительное время по конкретной технике (день × машина).
-- hours_idle / hours_repair / hours_not_in_service (= idle+repair); факт часов работы — контроль.
-- Источник статусов: dds.dim_equipment (SCD2). Без TRUNCATE — EXCHANGE.
DROP TABLE IF EXISTS dm.equipment_nonproductive_daily__tmp;
CREATE TABLE dm.equipment_nonproductive_daily__tmp AS dm.equipment_nonproductive_daily;

INSERT INTO dm.equipment_nonproductive_daily__tmp
(
    work_date,
    equipment_id,
    inventory_number,
    equipment_name,
    equipment_type,
    hours_idle,
    hours_repair,
    hours_not_in_service,
    work_hours_fact
)
SELECT
    s.work_date,
    s.equipment_id,
    any(s.inventory_number) AS inventory_number,
    any(s.equipment_name) AS equipment_name,
    any(s.equipment_type) AS equipment_type,
    round(sumIf(s.hours_piece, s.status = 'idle'), 2) AS hours_idle,
    round(sumIf(s.hours_piece, s.status = 'repair'), 2) AS hours_repair,
    round(
        sumIf(s.hours_piece, s.status = 'idle') + sumIf(s.hours_piece, s.status = 'repair'),
        2
    ) AS hours_not_in_service,
    round(ifNull(any(f.work_hours_fact), 0), 2) AS work_hours_fact
FROM
(
    SELECT
        e.equipment_id AS equipment_id,
        e.inventory_number AS inventory_number,
        e.name AS equipment_name,
        e.equipment_type AS equipment_type,
        e.status AS status,
        toDate(e.valid_from) + number AS work_date,
        greatest(
            0,
            dateDiff(
                'second',
                greatest(
                    e.valid_from,
                    toDateTime(toDate(e.valid_from) + number, 'Europe/Moscow')
                ),
                least(
                    e.valid_to,
                    toDateTime(toDate(e.valid_from) + number, 'Europe/Moscow') + toIntervalDay(1)
                )
            )
        ) / 3600.0 AS hours_piece
    FROM dds.dim_equipment AS e
    ARRAY JOIN range(
        toUInt32(
            greatest(
                0,
                dateDiff(
                    'day',
                    toDate(e.valid_from),
                    least(toDate(e.valid_to), today() + 1)
                )
            )
        )
    ) AS number
    WHERE e.status IN ('idle', 'repair')
      AND toDate(e.valid_from) < least(toDate(e.valid_to), today() + 1)
) AS s
LEFT JOIN
(
    SELECT
        toDate(started_at) AS work_date,
        equipment_id,
        sum(ifNull(work_hours, 0)) AS work_hours_fact
    FROM dds.fct_assignment
    GROUP BY
        work_date,
        equipment_id
) AS f
    ON s.work_date = f.work_date AND s.equipment_id = f.equipment_id
GROUP BY
    s.work_date,
    s.equipment_id
HAVING
    (sumIf(s.hours_piece, s.status = 'idle') + sumIf(s.hours_piece, s.status = 'repair')) > 0;

EXCHANGE TABLES dm.equipment_nonproductive_daily AND dm.equipment_nonproductive_daily__tmp;
DROP TABLE IF EXISTS dm.equipment_nonproductive_daily__tmp;
