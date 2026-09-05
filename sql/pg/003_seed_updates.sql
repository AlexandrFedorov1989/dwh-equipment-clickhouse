-- Обновления «как история» для демо SCD (после начального seed)

UPDATE dispatchers
SET phone = '+79990001122',
    updated_at = now()
WHERE id IN (1, 2, 3);

UPDATE dispatchers
SET shift_code = 'night',
    updated_at = now()
WHERE id IN (4, 5);

UPDATE equipment
SET status = 'repair',
    updated_at = now()
WHERE id IN (10, 20, 30);

UPDATE equipment
SET status = 'idle',
    dispatcher_id = 7,
    updated_at = now()
WHERE id IN (40, 50);
