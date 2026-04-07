CREATE TABLE IF NOT EXISTS state_registry (
    id SERIAL PRIMARY KEY,
    state_name TEXT NOT NULL UNIQUE,
    state_home_link TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS source_metadata (
    id SERIAL PRIMARY KEY,
    state_name TEXT NOT NULL,
    source_name TEXT NOT NULL,
    source_table_name TEXT NOT NULL,
    source_url TEXT NOT NULL,
    discovered_at TIMESTAMP NOT NULL DEFAULT NOW(),
    content_type TEXT,
    last_hash TEXT,
    last_seen_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_extracted_at TIMESTAMP,
    extraction_status TEXT NOT NULL DEFAULT 'discovered',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE (state_name, source_url)
);

CREATE TABLE IF NOT EXISTS mapping_column (
    id SERIAL PRIMARY KEY,
    state_name TEXT NOT NULL,
    source_name TEXT NOT NULL,
    source_url TEXT NOT NULL,
    raw_column TEXT NOT NULL,
    canonical_column TEXT NOT NULL,
    confidence NUMERIC(5,2) NOT NULL DEFAULT 0,
    rationale TEXT,
    approved BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (state_name, source_url, raw_column)
);

CREATE TABLE IF NOT EXISTS agent_memory (
    id SERIAL PRIMARY KEY,
    state_name TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    memory_key TEXT NOT NULL,
    memory_value TEXT NOT NULL,
    confidence NUMERIC(5,2) NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (state_name, agent_id, memory_key)
);

CREATE TABLE IF NOT EXISTS agent_handoff (
    id SERIAL PRIMARY KEY,
    state_name TEXT NOT NULL,
    from_agent TEXT NOT NULL,
    to_agent TEXT NOT NULL,
    message_type TEXT NOT NULL,
    message_body TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    acknowledged BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold_medicaid_rates (
    id BIGSERIAL PRIMARY KEY,
    state_id INTEGER NOT NULL,
    state_name TEXT NOT NULL,
    dataset_type TEXT NOT NULL,
    procedure_code TEXT NOT NULL,
    modifier TEXT NOT NULL DEFAULT '',
    description TEXT,
    fee_amount TEXT,
    effective_date TEXT,
    end_date DATE,
    source_url TEXT NOT NULL,
    ingestion_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    agent_version TEXT NOT NULL,
    row_hash TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_gold_rates_active_key
ON gold_medicaid_rates (state_id, dataset_type, procedure_code, modifier)
WHERE end_date IS NULL AND is_active = TRUE;

CREATE UNIQUE INDEX IF NOT EXISTS uq_gold_rates_version
ON gold_medicaid_rates (state_id, dataset_type, procedure_code, modifier, effective_date, row_hash);

CREATE TABLE IF NOT EXISTS canonical_column_mapping (
    id SERIAL PRIMARY KEY,
    dataset_type TEXT NOT NULL,
    reference_state TEXT NOT NULL,
    state_name TEXT NOT NULL,
    source_column_name TEXT NOT NULL,
    canonical_column_name TEXT NOT NULL,
    confidence NUMERIC(5,2) NOT NULL DEFAULT 0.9,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (dataset_type, reference_state, state_name, source_column_name)
);

INSERT INTO state_registry (state_name, state_home_link, is_active)
VALUES 
    (
        'alaska',
        'https://extranet-sp.dhss.alaska.gov/hcs/medicaidalaska/Provider/Sites/FeeSchedule.html',
        TRUE
    ),
    (
        'arizona',
        'https://www.azahcccs.gov/PlansProviders/RatesAndBilling/FFS/DurableMedEquip.html',
        TRUE
    )
ON CONFLICT (state_name) DO UPDATE
SET state_home_link = EXCLUDED.state_home_link,
    is_active = EXCLUDED.is_active;
