package com.demo.streaming.api;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/stats")
public class StatsController {

    private final JdbcTemplate jdbcTemplate;

    public StatsController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @GetMapping
    public Map<String, Object> getOverallStats() {
        Long activeAccounts = jdbcTemplate.queryForObject(
                "SELECT COUNT(DISTINCT account_id) FROM account_metrics_1min", Long.class);

        Long totalTransactions = jdbcTemplate.queryForObject(
                "SELECT COALESCE(SUM(transaction_count), 0) FROM account_metrics_1min", Long.class);

        Double totalVolume = jdbcTemplate.queryForObject(
                "SELECT COALESCE(SUM(total_amount), 0.0) FROM account_metrics_1min", Double.class);

        Long unresolvedFraudAlerts = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM fraud_alerts WHERE resolved = FALSE", Long.class);

        return Map.of(
                "activeAccounts", activeAccounts != null ? activeAccounts : 0,
                "totalTransactionsAggregated", totalTransactions != null ? totalTransactions : 0,
                "totalVolumeAggregatedUsd", totalVolume != null ? totalVolume : 0.0,
                "unresolvedFraudAlerts", unresolvedFraudAlerts != null ? unresolvedFraudAlerts : 0
        );
    }
}

