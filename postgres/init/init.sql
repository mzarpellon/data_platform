-- =========================
-- SCHEMAS
-- =========================
CREATE SCHEMA IF NOT EXISTS landing;
CREATE SCHEMA IF NOT EXISTS trusted;
CREATE SCHEMA IF NOT EXISTS refined;
CREATE SCHEMA IF NOT EXISTS metadata;

-- =========================
-- LANDING
-- Dados brutos, sem regra de negócio
-- =========================
CREATE TABLE IF NOT EXISTS landing.raw_events (
    id BIGSERIAL PRIMARY KEY,
    source_system VARCHAR(50) NOT NULL,
    ingestion_id UUID NOT NULL,
    payload JSONB NOT NULL,
    file_name VARCHAR(255),
    ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_events_ingestion_id
ON landing.raw_events (ingestion_id);

-- =========================
-- METADATA
-- Controle e auditoria
-- =========================
CREATE TABLE IF NOT EXISTS metadata.ingestion_control (
    ingestion_id UUID PRIMARY KEY,
    source_system VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    records_read INT,
    records_written INT,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS metadata.pipeline_runs (
    run_id BIGSERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100),
    execution_date TIMESTAMP,
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW()
);

-- =========================
-- TRUSTED
-- Dados tratados e tipados
-- =========================
CREATE TABLE IF NOT EXISTS trusted.events (
    event_id BIGSERIAL PRIMARY KEY,
    ingestion_id UUID NOT NULL,
    source_system VARCHAR(50),

    event_type VARCHAR(50) NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    event_date DATE NOT NULL,

    user_id BIGINT,
    success BOOLEAN,

    payload JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);


CREATE INDEX IF NOT EXISTS idx_trusted_events_event_date
ON trusted.events (event_date);

-- =========================
-- REFINED
-- Camada analítica
-- =========================
CREATE TABLE IF NOT EXISTS refined.daily_event_metrics (
    metric_date DATE PRIMARY KEY,
    source_system VARCHAR(50),
    total_events INT,
    created_at TIMESTAMP DEFAULT NOW()
);
