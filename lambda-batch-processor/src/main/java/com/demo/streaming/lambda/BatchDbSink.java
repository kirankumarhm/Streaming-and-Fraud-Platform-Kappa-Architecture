package com.demo.streaming.lambda;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;

/**
 * Persistence layer for writing pre-computed batch views into the Lambda serving store.
 */
public class BatchDbSink implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(BatchDbSink.class);
    private final HikariDataSource dataSource;

    public BatchDbSink(String jdbcUrl, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setPoolName("Lambda-Batch-Pool");
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(5000);
        this.dataSource = new HikariDataSource(config);
    }

    public void upsertBatchRollups(Collection<BatchAggregator.BatchRollup> rollups) {
        String sql = """
            INSERT INTO account_metrics_batch (
                account_id, batch_date, transaction_count, total_amount, max_amount, currency, reconciled_at
            ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (account_id, batch_date) DO UPDATE SET
                transaction_count = EXCLUDED.transaction_count,
                total_amount = EXCLUDED.total_amount,
                max_amount = EXCLUDED.max_amount,
                reconciled_at = CURRENT_TIMESTAMP;
        """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);

            for (BatchAggregator.BatchRollup r : rollups) {
                ps.setString(1, r.getAccountId());
                ps.setDate(2, Date.valueOf(r.getBatchDate()));
                ps.setInt(3, r.getTransactionCount());
                ps.setBigDecimal(4, r.getTotalAmount());
                ps.setBigDecimal(5, r.getMaxAmount());
                ps.setString(6, r.getCurrency());
                ps.addBatch();
            }

            ps.executeBatch();
            conn.commit();
            log.info("Batch layer successfully reconciled and flushed {} account batch records.", rollups.size());
        } catch (SQLException e) {
            log.error("Failed to persist batch rollups", e);
            throw new RuntimeException("Batch persistence failed", e);
        }
    }

    @Override
    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }
}

