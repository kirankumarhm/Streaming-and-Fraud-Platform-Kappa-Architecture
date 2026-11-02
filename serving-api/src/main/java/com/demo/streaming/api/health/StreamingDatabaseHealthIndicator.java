package com.demo.streaming.api.health;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Custom Actuator Health Indicator validating serving store responsiveness and schema readiness.
 */
@Component
public class StreamingDatabaseHealthIndicator implements HealthIndicator {

    private final JdbcTemplate jdbcTemplate;

    public StreamingDatabaseHealthIndicator(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public Health health() {
        try {
            Long count = jdbcTemplate.queryForObject("SELECT count(*) FROM account_metrics_1min", Long.class);
            return Health.up()
                    .withDetail("servingStore", "PostgreSQL")
                    .withDetail("status", "AVAILABLE")
                    .withDetail("accountMetricsRows", count != null ? count : 0)
                    .build();
        } catch (Exception e) {
            return Health.down()
                    .withDetail("servingStore", "PostgreSQL")
                    .withDetail("status", "UNREACHABLE")
                    .withDetail("error", e.getMessage())
                    .build();
        }
    }
}

