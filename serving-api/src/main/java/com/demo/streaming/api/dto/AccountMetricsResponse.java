package com.demo.streaming.api.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.swagger.v3.oas.annotations.media.Schema;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Representation DTO for account rolling window metrics.
 */
@Schema(description = "Account 1-minute rolling metrics snapshot")
public record AccountMetricsResponse(
        @Schema(description = "Customer account ID", example = "ACC-1002")
        String accountId,

        @Schema(description = "Window lower boundary")
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        Instant windowStart,

        @Schema(description = "Window upper boundary")
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        Instant windowEnd,

        @Schema(description = "Total transaction count in window", example = "8")
        int transactionCount,

        @Schema(description = "Cumulative spend volume in window", example = "1249.50")
        BigDecimal totalAmount,

        @Schema(description = "Max transaction amount observed in window", example = "499.00")
        BigDecimal maxAmount,

        @Schema(description = "Currency", example = "USD")
        String currency,

        @Schema(description = "Last update timestamp")
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        Instant updatedAt
) {}

