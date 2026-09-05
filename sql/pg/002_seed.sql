-- Тестовые данные:
--   80 диспетчеров (first_name + last_name)
--   200 единиц техники
--   assignments: каждый календарный день с 2025-08-01 по 2026-08-13,
--   в месяце ровно столько «дневных» слотов, сколько дней (28/29/30/31),
--   по 5 смен техники на день

INSERT INTO dispatchers (first_name, last_name, phone, shift_code, is_active, updated_at)
SELECT
    n.first_name,
    n.last_name || CASE
        WHEN n.first_name IN (
            'Таня', 'Маша', 'Даша', 'Оля', 'Лена', 'Наташа', 'Катя', 'Юля',
            'Аня', 'Вика', 'Света', 'Настя', 'Ира', 'Полина', 'Алина', 'Вера',
            'Лиза', 'Ксюша', 'Рита', 'Зоя'
        ) THEN 'а'
        ELSE ''
    END,
    format('+7900%s', lpad(n.g::text, 7, '0')),
    CASE WHEN n.g % 2 = 0 THEN 'day' ELSE 'night' END,
    (n.g % 19 <> 0),
    timestamptz '2025-08-01 09:00:00+03' + ((n.g - 1) * interval '3 days')
FROM (
    SELECT
        g,
        (ARRAY[
            'Таня', 'Ваня', 'Коля', 'Маша', 'Саша', 'Даша', 'Петя', 'Оля',
            'Дима', 'Лена', 'Игорь', 'Наташа', 'Серёжа', 'Катя', 'Андрей', 'Юля',
            'Максим', 'Аня', 'Паша', 'Вика', 'Артём', 'Света', 'Рома', 'Настя',
            'Кирилл', 'Ира', 'Денис', 'Полина', 'Никита', 'Алина', 'Егор', 'Вера',
            'Тимур', 'Лиза', 'Глеб', 'Ксюша', 'Борис', 'Рита', 'Фёдор', 'Зоя'
        ])[1 + ((g - 1) % 40)] AS first_name,
        (ARRAY[
            'Иванов', 'Петров', 'Сидоров', 'Смирнов', 'Кузнецов', 'Попов', 'Васильев',
            'Новиков', 'Фёдоров', 'Морозов', 'Волков', 'Алексеев', 'Лебедев', 'Семёнов',
            'Егоров', 'Павлов', 'Козлов', 'Степанов', 'Николаев', 'Орлов', 'Андреев',
            'Макаров', 'Никитин', 'Захаров', 'Зайцев', 'Соловьёв', 'Борисов', 'Яковлев',
            'Григорьев', 'Романов', 'Воробьёв', 'Сергеев', 'Кузьмин', 'Фролов', 'Александров',
            'Дмитриев', 'Королёв', 'Гусев', 'Киселёв', 'Ильин'
        ])[1 + ((g - 1) % 40)] AS last_name
    FROM generate_series(1, 80) AS g
) AS n;

INSERT INTO equipment (
    inventory_number,
    name,
    equipment_type,
    status,
    dispatcher_id,
    updated_at
)
SELECT
    format('INV-%s', lpad(g::text, 4, '0')),
    format(
        '%s-%s',
        (ARRAY['Экскаватор', 'Самосвал', 'Погрузчик', 'Бульдозер', 'Кран'])[1 + ((g - 1) % 5)],
        g
    ),
    (ARRAY['excavator', 'dump_truck', 'loader', 'bulldozer', 'crane'])[1 + ((g - 1) % 5)],
    (ARRAY['in_service', 'in_service', 'in_service', 'repair', 'idle'])[1 + ((g - 1) % 5)],
    1 + ((g - 1) % 80),
    timestamptz '2025-08-01 09:00:00+03' + ((g - 1) * interval '12 hours')
FROM generate_series(1, 200) AS g;

INSERT INTO equipment_assignments (
    equipment_id,
    dispatcher_id,
    started_at,
    ended_at,
    work_hours,
    created_at
)
SELECT
    1 + ((EXTRACT(DOY FROM d.work_date)::int + s.slot - 2) % 200),
    1 + ((EXTRACT(DOY FROM d.work_date)::int + s.slot - 2) % 80),
    ((d.work_date + time '08:00') + ((s.slot - 1) * interval '1 hour'))
        AT TIME ZONE 'Europe/Moscow',
    CASE
        WHEN d.work_date = DATE '2026-08-13' AND s.slot = 5 THEN NULL
        ELSE ((d.work_date + time '08:00') + ((s.slot - 1) * interval '1 hour') + interval '8 hours')
            AT TIME ZONE 'Europe/Moscow'
    END,
    CASE
        WHEN d.work_date = DATE '2026-08-13' AND s.slot = 5 THEN NULL
        ELSE round((6 + ((s.slot + EXTRACT(DAY FROM d.work_date)::int) % 5))::numeric, 2)
    END,
    ((d.work_date + time '07:50')) AT TIME ZONE 'Europe/Moscow'
FROM generate_series(DATE '2025-08-01', DATE '2026-08-13', INTERVAL '1 day') AS d(work_date)
CROSS JOIN generate_series(1, 5) AS s(slot);
