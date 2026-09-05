-- DM: нагрузка диспетчеров (день × диспетчер). Полный пересчёт через EXCHANGE (без TRUNCATE).
DROP TABLE IF EXISTS dm.dispatcher_workload_daily__tmp;
CREATE TABLE dm.dispatcher_workload_daily__tmp AS dm.dispatcher_workload_daily;

INSERT INTO dm.dispatcher_workload_daily__tmp
(
    work_date,
    dispatcher_id,
    dispatcher_first_name,
    dispatcher_last_name,
    shift_code,
    assignments_cnt,
    equipment_cnt,
    work_hours_sum
)
SELECT
    toDate(f.started_at) AS work_date,
    f.dispatcher_id,
    any(d.first_name) AS dispatcher_first_name,
    any(d.last_name) AS dispatcher_last_name,
    any(d.shift_code) AS shift_code,
    toUInt32(count()) AS assignments_cnt,
    toUInt32(uniqExact(f.equipment_id)) AS equipment_cnt,
    sum(ifNull(f.work_hours, 0)) AS work_hours_sum
FROM dds.fct_assignment AS f
INNER JOIN dds.dim_dispatcher AS d ON f.dispatcher_sk = d.dispatcher_sk
GROUP BY
    work_date,
    f.dispatcher_id;

EXCHANGE TABLES dm.dispatcher_workload_daily AND dm.dispatcher_workload_daily__tmp;
DROP TABLE IF EXISTS dm.dispatcher_workload_daily__tmp;
