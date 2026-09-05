-- Холодный tier: только TTL / storage_policy (без DROP).
-- Перенос старше 15 месяцев → s3_cold делает ClickHouse (TTL TO VOLUME), не DAG.
-- Скользящее окно: стык 1 января не «роняет» свежий декабрь на cold.
-- Свежий стенд: CREATE в 002/004 уже с PARTITION BY + TTL.

ALTER TABLE ods.equipment_assignments
    MODIFY SETTING storage_policy = 'tiered';

ALTER TABLE ods.equipment_assignments
    MODIFY TTL started_at + INTERVAL 15 MONTH TO VOLUME 'cold';

ALTER TABLE dds.fct_assignment
    MODIFY SETTING storage_policy = 'tiered';

ALTER TABLE dds.fct_assignment
    MODIFY TTL started_at + INTERVAL 15 MONTH TO VOLUME 'cold';

ALTER TABLE dm.equipment_utilization_daily
    MODIFY SETTING storage_policy = 'tiered';

ALTER TABLE dm.equipment_utilization_daily
    MODIFY TTL work_date + INTERVAL 15 MONTH TO VOLUME 'cold';

ALTER TABLE dm.dispatcher_workload_daily
    MODIFY SETTING storage_policy = 'tiered';

ALTER TABLE dm.dispatcher_workload_daily
    MODIFY TTL work_date + INTERVAL 15 MONTH TO VOLUME 'cold';

ALTER TABLE dm.equipment_nonproductive_daily
    MODIFY SETTING storage_policy = 'tiered';

ALTER TABLE dm.equipment_nonproductive_daily
    MODIFY TTL work_date + INTERVAL 15 MONTH TO VOLUME 'cold';
