package com.demo.streaming.api.repository;

import com.demo.streaming.api.dto.FraudAlertResponse;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

@Repository
public class FraudAlertRepositoryImpl implements FraudAlertRepository {

    private final JdbcTemplate jdbcTemplate;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public FraudAlertRepositoryImpl(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    private static final RowMapper<FraudAlertResponse> ROW_MAPPER = new RowMapper<>() {
        @Override
        public FraudAlertResponse mapRow(ResultSet rs, int rowNum) throws SQLException {
            Map<String, Object> metadata = Collections.emptyMap();
            String metadataJson = rs.getString("metadata");
            if (metadataJson != null && !metadataJson.isBlank()) {
                try {
                    metadata = MAPPER.readValue(metadataJson, new TypeReference<>() {});
                } catch (Exception ignored) {}
            }

            return new FraudAlertResponse(
                    rs.getString("alert_id"),
                    rs.getString("transaction_id"),
                    rs.getString("account_id"),
                    rs.getString("rule_name"),
                    rs.getString("severity"),
                    rs.getString("reason"),
                    metadata,
                    rs.getTimestamp("detected_at").toInstant(),
                    rs.getBoolean("resolved")
            );
        }
    };

    @Override
    public List<FraudAlertResponse> findAlerts(boolean includeResolved, String severity, int limit) {
        StringBuilder sql = new StringBuilder("""
            SELECT alert_id, transaction_id, account_id, rule_name, severity, reason, metadata, detected_at, resolved
            FROM fraud_alerts
            WHERE 1=1
        """);

        List<Object> params = new ArrayList<>();

        if (!includeResolved) {
            sql.append(" AND resolved = FALSE");
        }
        if (severity != null && !severity.isBlank()) {
            sql.append(" AND severity = ?");
            params.add(severity.toUpperCase());
        }

        sql.append(" ORDER BY detected_at DESC LIMIT ?");
        params.add(limit);

        return jdbcTemplate.query(sql.toString(), ROW_MAPPER, params.toArray());
    }

    @Override
    public Optional<FraudAlertResponse> findById(String alertId) {
        String sql = """
            SELECT alert_id, transaction_id, account_id, rule_name, severity, reason, metadata, detected_at, resolved
            FROM fraud_alerts
            WHERE alert_id = ?
        """;
        try {
            FraudAlertResponse response = jdbcTemplate.queryForObject(sql, ROW_MAPPER, alertId);
            return Optional.ofNullable(response);
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    @Override
    public boolean markResolved(String alertId, String note, String resolvedBy) {
        String sql = "UPDATE fraud_alerts SET resolved = TRUE WHERE alert_id = ? AND resolved = FALSE";
        int updated = jdbcTemplate.update(sql, alertId);
        return updated > 0;
    }
}

