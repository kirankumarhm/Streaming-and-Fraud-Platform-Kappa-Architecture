-- Serving Layer Database Schema for Real-Time Payment & Fraud Analytics (Kappa Architecture)

CREATE TABLE IF NOT EXISTS account_metrics_1min (
    account_id VARCHAR(64) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    transaction_count INT NOT NULL DEFAULT 0,
    total_amount NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
    max_amount NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
    currency VARCHAR(10) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, window_start)
);

CREATE TABLE IF NOT EXISTS account_metrics_10min (
    account_id VARCHAR(64) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    transaction_count INT NOT NULL DEFAULT 0,
    total_amount NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
    max_amount NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
    currency VARCHAR(10) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, window_start)
);

CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id VARCHAR(64) PRIMARY KEY,
    transaction_id VARCHAR(64) NOT NULL,
    account_id VARCHAR(64) NOT NULL,
    rule_name VARCHAR(128) NOT NULL,
    severity VARCHAR(20) NOT NULL, -- LOW, MEDIUM, HIGH, CRITICAL
    reason TEXT NOT NULL,
    metadata JSONB,
    detected_at TIMESTAMP NOT NULL,
    resolved BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_fraud_account ON fraud_alerts (account_id);
CREATE INDEX IF NOT EXISTS idx_fraud_detected ON fraud_alerts (detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_account_window ON account_metrics_1min (account_id, window_end DESC);

