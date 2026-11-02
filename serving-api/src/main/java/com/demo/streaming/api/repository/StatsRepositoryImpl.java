package com.demo.streaming.api.repository;

import com.demo.streaming.api.dto.OverallStatsResponse;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;

@Repository
public class StatsRepositoryImpl implements StatsRepository {

    private final JdbcTemplate jdbcTemplate;

    public StatsRepositoryImpl(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public OverallStatsResponse getSystemSummary() {
        Long activeAccounts = jdbcTemplate.queryForObject(
                "SELECT COUNT(DISTINCT account_id) FROM account_metrics_1min", Long.class);

        Long totalTransactions = jdbcTemplate.queryForObject(
                "SELECT COALESCE(SUM(transaction_count), 0) FROM account_metrics_1min", Long.class);

        BigDecimal totalVolume = jdbcTemplate.queryForObject(
                "SELECT COALESCE(SUM(total_amount), 0.00) FROM account_metrics_1min", BigDecimal.class);

        Long unresolvedFraudAlerts = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM fraud_alerts WHERE resolved = FALSE", Long.class);

        return new OverallStatsResponse(
                activeAccounts != null ? activeAccounts : 0L,
                totalTransactions != null ? totalTransactions : 0L,
                totalVolume != null ? totalVolume : BigDecimal.ZERO,
                unresolvedFraudAlerts != null ? unresolvedFraudAlerts : 0L
        );
    }
}

