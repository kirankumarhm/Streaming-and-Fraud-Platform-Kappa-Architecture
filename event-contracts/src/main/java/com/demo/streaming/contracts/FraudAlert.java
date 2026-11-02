package com.demo.streaming.contracts;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable canonical entity representing a detected anomaly or rule violation.
 *
 * @param alertId       Unique surrogate key for the alert.
 * @param transactionId Reference to the triggering transaction.
 * @param accountId     Customer account flagged.
 * @param ruleName      Canonical name of the triggered fraud rule.
 * @param severity      Risk classification (LOW, MEDIUM, HIGH, CRITICAL).
 * @param reason        Human-readable explanation of why the rule fired.
 * @param metadata      Contextual telemetry payload (e.g. distance, burst count, amounts).
 * @param detectedAt    Timestamp of alert creation.
 * @param resolved      Resolution lifecycle state.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record FraudAlert(
        @NotBlank String alertId,
        @NotBlank String transactionId,
        @NotBlank String accountId,
        @NotBlank String ruleName,
        @NotNull Severity severity,
        @NotBlank String reason,
        Map<String, Object> metadata,
        @NotNull
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        Instant detectedAt,
        boolean resolved
) {
    public FraudAlert {
        Objects.requireNonNull(alertId, "alertId cannot be null");
        Objects.requireNonNull(transactionId, "transactionId cannot be null");
        Objects.requireNonNull(accountId, "accountId cannot be null");
        Objects.requireNonNull(ruleName, "ruleName cannot be null");
        Objects.requireNonNull(severity, "severity cannot be null");
        Objects.requireNonNull(reason, "reason cannot be null");
        Objects.requireNonNull(detectedAt, "detectedAt cannot be null");

        // Ensure defensive immutability for metadata map
        metadata = (metadata == null) ? Collections.emptyMap() : Collections.unmodifiableMap(metadata);
    }

    /**
     * Factory method for creating unresolved alerts.
     */
    public static FraudAlert createUnresolved(
            String alertId,
            String transactionId,
            String accountId,
            String ruleName,
            Severity severity,
            String reason,
            Map<String, Object> metadata,
            Instant detectedAt) {
        return new FraudAlert(alertId, transactionId, accountId, ruleName, severity, reason, metadata, detectedAt, false);
    }

    public enum Severity {
        LOW,
        MEDIUM,
        HIGH,
        CRITICAL
    }
}
