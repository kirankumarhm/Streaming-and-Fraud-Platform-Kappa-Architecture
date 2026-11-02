package com.demo.streaming.api.controller;

import com.demo.streaming.api.dto.ApiResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Demonstrates the Lambda Architecture Serving Layer.
 * In Lambda Architecture, queries are answered by merging the historical pre-computed
 * Batch View with the low-latency Speed View:
 * Query = Merge(Batch View, Real-Time Speed View)
 */
@RestController
@RequestMapping("/api/v1/lambda")
@Tag(name = "Lambda Architecture Serving Layer", description = "Endpoints demonstrating Lambda Architecture's union of Batch Layer and Speed Layer")
public class LambdaMergedViewController {

    private final JdbcTemplate jdbcTemplate;

    public LambdaMergedViewController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @GetMapping("/accounts/{accountId}/merged-view")
    @Operation(
            summary = "Get Lambda Merged View for an Account",
            description = "Applies the classic Lambda Architecture formula: Query = Merge(Batch View, Real-Time Speed View)."
    )
    public ApiResponse<Map<String, Object>> getMergedView(
            @Parameter(description = "Customer Account ID", example = "ACC-1002")
            @PathVariable String accountId) {

        // 1. Query Batch Layer (Historical Authoritative Pre-computed View)
        String batchSql = """
            SELECT batch_date, transaction_count, total_amount, max_amount, currency, reconciled_at
            FROM account_metrics_batch
            WHERE account_id = ?
            ORDER BY batch_date DESC
        """;
        List<Map<String, Object>> batchViews = jdbcTemplate.queryForList(batchSql, accountId);

        // 2. Query Speed Layer (Real-Time Tumbling Window Streaming View)
        String speedSql = """
            SELECT window_start, window_end, transaction_count, total_amount, max_amount, currency
            FROM account_metrics_1min
            WHERE account_id = ?
            ORDER BY window_start DESC
            LIMIT 10
        """;
        List<Map<String, Object>> speedViews = jdbcTemplate.queryForList(speedSql, accountId);

        // 3. Compute Merged Rollup
        int totalTransactions = 0;
        BigDecimal totalVolume = BigDecimal.ZERO;

        for (Map<String, Object> b : batchViews) {
            totalTransactions += ((Number) b.get("transaction_count")).intValue();
            totalVolume = totalVolume.add((BigDecimal) b.get("total_amount"));
        }

        for (Map<String, Object> s : speedViews) {
            totalTransactions += ((Number) s.get("transaction_count")).intValue();
            totalVolume = totalVolume.add((BigDecimal) s.get("total_amount"));
        }

        Map<String, Object> mergedResult = Map.of(
                "accountId", accountId,
                "architecturePattern", "Lambda Architecture (Dual-Pipeline)",
                "formula", "Query = Merge(BatchLayer.account_metrics_batch, SpeedLayer.account_metrics_1min)",
                "totalAggregatedTransactions", totalTransactions,
                "totalAggregatedVolumeUsd", totalVolume,
                "batchLayerRecords", batchViews,
                "speedLayerRecords", speedViews
        );

        return ApiResponse.success(mergedResult, "Lambda merged view computed successfully");
    }
}

