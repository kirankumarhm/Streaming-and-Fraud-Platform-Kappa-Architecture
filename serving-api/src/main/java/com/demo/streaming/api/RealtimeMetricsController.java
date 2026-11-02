package com.demo.streaming.api;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/accounts")
public class RealtimeMetricsController {

    private final JdbcTemplate jdbcTemplate;

    public RealtimeMetricsController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @GetMapping("/{accountId}/metrics")
    public List<Map<String, Object>> getAccountMetrics(@PathVariable String accountId) {
        String sql = """
            SELECT account_id, window_start, window_end, transaction_count, total_amount, max_amount, currency, updated_at
            FROM account_metrics_1min
            WHERE account_id = ?
            ORDER BY window_start DESC
            LIMIT 20
        """;
        return jdbcTemplate.queryForList(sql, accountId);
    }
}

