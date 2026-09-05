-- Схема OLTP-источника (dispatchers / equipment / assignments)

CREATE TABLE IF NOT EXISTS dispatchers (
    id          serial PRIMARY KEY,
    first_name  text        NOT NULL,
    last_name   text        NOT NULL,
    phone       text,
    shift_code  text        NOT NULL DEFAULT 'day',
    is_active   boolean     NOT NULL DEFAULT true,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS equipment (
    id                serial PRIMARY KEY,
    inventory_number  text        NOT NULL UNIQUE,
    name              text        NOT NULL,
    equipment_type    text        NOT NULL,
    status            text        NOT NULL DEFAULT 'in_service'
                      CHECK (status IN ('in_service', 'repair', 'idle')),
    dispatcher_id     int         REFERENCES dispatchers (id),
    updated_at        timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS equipment_assignments (
    id             bigserial PRIMARY KEY,
    equipment_id   int         NOT NULL REFERENCES equipment (id),
    dispatcher_id  int         NOT NULL REFERENCES dispatchers (id),
    started_at     timestamptz NOT NULL,
    ended_at       timestamptz,
    work_hours     numeric(8, 2),
    created_at     timestamptz NOT NULL DEFAULT now(),
    CHECK (ended_at IS NULL OR ended_at >= started_at)
);

CREATE INDEX IF NOT EXISTS idx_equipment_dispatcher_id
    ON equipment (dispatcher_id);

CREATE INDEX IF NOT EXISTS idx_equipment_updated_at
    ON equipment (updated_at);

CREATE INDEX IF NOT EXISTS idx_dispatchers_updated_at
    ON dispatchers (updated_at);

CREATE INDEX IF NOT EXISTS idx_assignments_started_at
    ON equipment_assignments (started_at);

CREATE INDEX IF NOT EXISTS idx_assignments_equipment_id
    ON equipment_assignments (equipment_id);

-- Автообновление updated_at при любом UPDATE (нужно для инкремента ODS)
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_dispatchers_updated_at ON dispatchers;
CREATE TRIGGER trg_dispatchers_updated_at
    BEFORE UPDATE ON dispatchers
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_equipment_updated_at ON equipment;
CREATE TRIGGER trg_equipment_updated_at
    BEFORE UPDATE ON equipment
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();
