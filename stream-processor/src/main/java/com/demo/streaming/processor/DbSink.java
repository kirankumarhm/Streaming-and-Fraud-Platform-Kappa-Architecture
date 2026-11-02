package com.demo.streaming.processor;

import com.demo.streaming.contracts.AccountMetricsWindow;
import com.demo.streaming.contracts.FraudAlert;
import com.demo.streaming.processor.exception.SinkPersistenceException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Objects;

/**
 * Production database sink maintaining a tuned HikariCP connection pool.
 * Guarantees idempotent upserts for window metrics and fraud alerts.
 */
public class DbSink implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(DbSink.class);
    private final HikariDataSource dataSource;
    private final ObjectMapper mapper = new ObjectMapper();

    public DbSink(String jdbcUrl, String username, String password) {
        Objects.requireNonNull(jdbcUrl, "jdbcUrl is mandatory");
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        
        // Production pool tuning
        config.setPoolName("Kappa-HikariPool");
        config.setMaximumPoolSize(15);
        config.setMinimumIdle(3);
        config.setConnectionTimeout(5000);
        config.setIdleTimeout(300000);
        config.setMaxLifetime(1800000);
        config.setLeakDetectionThreshold(15000);

        this.dataSource = new HikariDataSource(config);
        log.info("HikariCP connection pool initialized for target DB at {}", jdbcUrl);
    }

    public void upsertAccountMetrics(AccountMetricsWindow metrics) {
        String sql = """
            INSERT INTO account_metrics_1min (
                account_id, window_start, window_end, transaction_count, total_amount, max_amount, currency, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (account_id, window_start) DO UPDATE SET
                transaction_count = EXCLUDED.transaction_count,
                total_amount = EXCLUDED.total_amount,
                max_amount = EXCLUDED.max_amount,
                updated_at = CURRENT_TIMESTAMP;
        """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, metrics.accountId());
            ps.setTimestamp(2, Timestamp.from(metrics.windowStart()));
            ps.setTimestamp(3, Timestamp.from(metrics.windowEnd()));
            ps.setInt(4, metrics.transactionCount());
            ps.setBigDecimal(5, metrics.totalAmount());
            ps.setBigDecimal(6, metrics.maxAmount());
            ps.setString(7, metrics.currency());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new SinkPersistenceException("Failed to persist window metrics for account: " + metrics.accountId(), e);
        }
    }

    public void insertFraudAlert(FraudAlert alert) {
        String sql = """
            INSERT INTO fraud_alerts (
                alert_id, transaction_id, account_id, rule_name, severity, reason, metadata, detected_at, resolved
            ) VALUES (?, ?, ?, ?, ?, ?, ?::jsonb, ?, FALSE)
            ON CONFLICT (alert_id) DO NOTHING;
        """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, alert.alertId());
            ps.setString(2, alert.transactionId());
            ps.setString(3, alert.accountId());
            ps.setString(4, alert.ruleName());
            ps.setString(5, alert.severity().name());
            ps.setString(6, alert.reason());
            ps.setString(7, mapper.writeValueAsString(alert.metadata()));
            ps.setTimestamp(8, Timestamp.from(alert.detectedAt()));
            ps.executeUpdate();

            log.warn("🚨 [FRAUD DETECTED] ID={} Account={} Rule={} Severity={}",
                    alert.alertId(), alert.accountId(), alert.ruleName(), alert.severity());
        } catch (Exception e) {
            throw new SinkPersistenceException("Failed to persist fraud alert: " + alert.alertId(), e);
        }
    }

    @Override
    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            log.info("HikariCP connection pool shut down cleanly.");
        }
    }
}
