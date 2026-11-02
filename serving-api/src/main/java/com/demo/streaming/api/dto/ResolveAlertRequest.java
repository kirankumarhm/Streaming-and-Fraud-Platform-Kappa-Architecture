package com.demo.streaming.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/**
 * Inbound payload for resolving a fraud incident.
 */
@Schema(description = "Fraud alert resolution payload")
public record ResolveAlertRequest(
        @NotBlank(message = "Resolution note is mandatory")
        @Size(min = 5, max = 500, message = "Resolution note must be between 5 and 500 characters")
        @Schema(description = "Triage explanation", example = "Verified with customer; legitimate overseas purchase.")
        String note,

        @NotBlank(message = "Agent identifier is required")
        @Schema(description = "Operator or automated system resolving the alert", example = "fraud_analyst_42")
        String resolvedBy
) {}

