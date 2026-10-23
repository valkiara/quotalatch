CREATE TABLE IF NOT EXISTS {schema}.schema_version (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    version integer NOT NULL,
    checksum text NOT NULL
);

CREATE TABLE {schema}.budgets (
    tenant_id text NOT NULL,
    budget_id text NOT NULL,
    limit_micro numeric(38, 0) NOT NULL CHECK (limit_micro >= 0),
    spent_micro numeric(38, 0) NOT NULL DEFAULT 0 CHECK (spent_micro >= 0),
    held_micro numeric(38, 0) NOT NULL DEFAULT 0 CHECK (held_micro >= 0),
    PRIMARY KEY (tenant_id, budget_id)
);

CREATE TABLE {schema}.operations (
    tenant_id text NOT NULL,
    operation_id uuid NOT NULL,
    operation_key text NOT NULL,
    request_fingerprint text NOT NULL,
    request_hash text NOT NULL,
    status text NOT NULL CHECK (status IN ('reserved','dispatched','unknown','settled','cancelled')),
    bound_micro numeric(38, 0) NOT NULL CHECK (bound_micro >= 0),
    actual_micro numeric(38, 0) CHECK (actual_micro >= 0),
    dispatch_token uuid NOT NULL,
    expires_at timestamptz NOT NULL,
    receipt_ref text,
    unknown_reason text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, operation_id),
    UNIQUE (tenant_id, operation_key),
    CHECK ((status = 'settled') = (actual_micro IS NOT NULL)),
    CHECK ((status = 'settled') = (receipt_ref IS NOT NULL)),
    CHECK (status <> 'unknown' OR unknown_reason IS NOT NULL)
);

CREATE TABLE {schema}.operation_budgets (
    tenant_id text NOT NULL,
    operation_id uuid NOT NULL,
    budget_id text NOT NULL,
    PRIMARY KEY (tenant_id, operation_id, budget_id),
    FOREIGN KEY (tenant_id, operation_id) REFERENCES {schema}.operations,
    FOREIGN KEY (tenant_id, budget_id) REFERENCES {schema}.budgets
);

CREATE TABLE {schema}.events (
    event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id text NOT NULL,
    operation_id uuid NOT NULL,
    kind text NOT NULL,
    details jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (tenant_id, operation_id) REFERENCES {schema}.operations
);

CREATE INDEX operations_recovery ON {schema}.operations (tenant_id, expires_at)
    WHERE status IN ('reserved', 'dispatched');
CREATE INDEX events_operation ON {schema}.events (tenant_id, operation_id, event_id);

CREATE FUNCTION {schema}.reject_event_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'quotalatch events are append-only';
END;
$$;
CREATE TRIGGER immutable_events BEFORE UPDATE OR DELETE OR TRUNCATE ON {schema}.events
    FOR EACH STATEMENT EXECUTE FUNCTION {schema}.reject_event_mutation();
