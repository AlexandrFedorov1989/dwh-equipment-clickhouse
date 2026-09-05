-- DM: загрузка парка (день × техника) — только факт работы (часы/выезды).
-- Статус idle/repair сюда НЕ кладём (это витрина 3): иначе «топ по работе» путается со справочным статусом.
-- Полный пересчёт через EXCHANGE (без TRUNCATE): tmp → INSERT → атомарная подмена.
DROP TABLE IF EXISTS dm.equipment_utilization_daily__tmp;
CREATE TABLE dm.equipment_utilization_daily__tmp AS dm.equipment_utilization_daily;

INSERT INTO dm.equipment_utilization_daily__tmp
(
    work_date,
    equipment_id,
    inventory_number,
    equipment_name,
    equipment_type,
    dispatcher_id,
    dispatcher_first_name,
    dispatcher_last_name,
    shift_code,
    assignments_cnt,
    work_hours_sum
)
SELECT
    toDate(f.started_at) AS work_date,
    f.equipment_id,
    any(e.inventory_number) AS inventory_number,
    any(e.name) AS equipment_name,
    any(e.equipment_type) AS equipment_type,
    any(f.dispatcher_id) AS dispatcher_id,
    any(d.first_name) AS dispatcher_first_name,
    any(d.last_name) AS dispatcher_last_name,
    any(d.shift_code) AS shift_code,
    toUInt32(count()) AS assignments_cnt,
    sum(ifNull(f.work_hours, 0)) AS work_hours_sum
FROM dds.fct_assignment AS f
INNER JOIN dds.dim_equipment AS e ON f.equipment_sk = e.equipment_sk
INNER JOIN dds.dim_dispatcher AS d ON f.dispatcher_sk = d.dispatcher_sk
GROUP BY
    work_date,
    f.equipment_id;

EXCHANGE TABLES dm.equipment_utilization_daily AND dm.equipment_utilization_daily__tmp;
DROP TABLE IF EXISTS dm.equipment_utilization_daily__tmp;
