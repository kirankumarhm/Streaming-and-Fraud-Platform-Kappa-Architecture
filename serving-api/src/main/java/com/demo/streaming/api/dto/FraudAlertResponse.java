package com.demo.streaming.api.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.Map;

/**
 * Representation DTO for fraud alert triage.
 */
@Schema(description = "Fraud alert incident representation")
public record FraudAlertResponse(
        @Schema(description = "Surrogate alert identifier", example = "3f82bb19-4821-4209-90bc-234208a09b44")
        String alertId,

        @Schema(description = "Triggering transaction UUID", example = "9b9087ea-3192-4d22-bf4b-e3c66f4cb418")
        String transactionId,

        @Schema(description = "Flagged account ID", example = "ACC-1005")
        String accountId,

        @Schema(description = "Triggered rule", example = "RULE_IMPOSSIBLE_TRAVEL")
        String ruleName,

        @Schema(description = "Severity tier", example = "CRITICAL")
        String severity,

        @Schema(description = "Human-readable explanation", example = "Impossible displacement: 5570 km traversed in 1 second")
        String reason,

        @Schema(description = "Rule context metadata")
        Map<String, Object> metadata,

        @Schema(description = "Detection timestamp")
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        Instant detectedAt,

        @Schema(description = "Resolution status", example = "false")
        boolean resolved
) {}

