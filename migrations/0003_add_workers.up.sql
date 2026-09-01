CREATE TYPE worker_status AS ENUM ('ACTIVE', 'IDLE', 'UNHEALTHY', 'OFFLINE');

CREATE TABLE workers (
    id             UUID PRIMARY KEY,
    host           TEXT NOT NULL,
    status         worker_status NOT NULL DEFAULT 'IDLE',
    cpu            DOUBLE PRECISION,
    memory         DOUBLE PRECISION,
    running_jobs   INT NOT NULL DEFAULT 0,
    last_heartbeat TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);