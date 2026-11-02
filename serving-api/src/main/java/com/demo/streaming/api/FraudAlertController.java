package com.demo.streaming.api;

import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/fraud")
public class FraudAlertController {

    private final JdbcTemplate jdbcTemplate;

    public FraudAlertController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @GetMapping("/alerts")
    public List<Map<String, Object>> getAlerts(
            @RequestParam(required = false, defaultValue = "false") boolean includeResolved,
            @RequestParam(required = false) String severity) {

        StringBuilder sql = new StringBuilder("""
            SELECT alert_id, transaction_id, account_id, rule_name, severity, reason, metadata, detected_at, resolved
            FROM fraud_alerts
            WHERE 1=1
        """);

        if (!includeResolved) {
            sql.append(" AND resolved = FALSE");
        }
        if (severity != null && !severity.isBlank()) {
            sql.append(" AND severity = '").append(severity.toUpperCase()).append("'");
        }
        sql.append(" ORDER BY detected_at DESC LIMIT 50");

        return jdbcTemplate.queryForList(sql.toString());
    }

    @PostMapping("/alerts/{alertId}/resolve")
    public ResponseEntity<Map<String, Object>> resolveAlert(@PathVariable String alertId) {
        int updated = jdbcTemplate.update("UPDATE fraud_alerts SET resolved = TRUE WHERE alert_id = ?", alertId);
        if (updated > 0) {
            return ResponseEntity.ok(Map.of("status", "SUCCESS", "alertId", alertId, "resolved", true));
        } else {
            return ResponseEntity.notFound().build();
        }
    }
}

