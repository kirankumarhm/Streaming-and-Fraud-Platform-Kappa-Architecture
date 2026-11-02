package com.demo.streaming.api.repository;

import com.demo.streaming.api.dto.AccountMetricsResponse;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Repository
public class AccountMetricsRepositoryImpl implements AccountMetricsRepository {

    private final JdbcTemplate jdbcTemplate;

    public AccountMetricsRepositoryImpl(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    private static final RowMapper<AccountMetricsResponse> ROW_MAPPER = new RowMapper<>() {
        @Override
        public AccountMetricsResponse mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new AccountMetricsResponse(
                    rs.getString("account_id"),
                    rs.getTimestamp("window_start").toInstant(),
                    rs.getTimestamp("window_end").toInstant(),
                    rs.getInt("transaction_count"),
                    rs.getBigDecimal("total_amount"),
                    rs.getBigDecimal("max_amount"),
                    rs.getString("currency"),
                    rs.getTimestamp("updated_at").toInstant()
            );
        }
    };

    @Override
    public List<AccountMetricsResponse> findLatestMetricsByAccountId(String accountId, int limit) {
        String sql = """
            SELECT account_id, window_start, window_end, transaction_count, total_amount, max_amount, currency, updated_at
            FROM account_metrics_1min
            WHERE account_id = ?
            ORDER BY window_start DESC
            LIMIT ?
        """;
        return jdbcTemplate.query(sql, ROW_MAPPER, accountId, limit);
    }
}

