package com.demo.streaming.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import java.math.BigDecimal;

/**
 * High-level system statistics summary.
 */
@Schema(description = "Overall system statistics summary")
public record OverallStatsResponse(
        @Schema(description = "Count of active distinct accounts", example = "10")
        long activeAccounts,

        @Schema(description = "Total transactions aggregated", example = "4250")
        long totalTransactionsAggregated,

        @Schema(description = "Total monetary volume in USD", example = "194025.50")
        BigDecimal totalVolumeAggregatedUsd,

        @Schema(description = "Count of pending unresolved fraud alerts", example = "5")
        long unresolvedFraudAlerts
) {}

