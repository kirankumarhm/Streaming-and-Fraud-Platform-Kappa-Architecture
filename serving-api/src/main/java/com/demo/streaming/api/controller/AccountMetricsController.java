package com.demo.streaming.api.controller;

import com.demo.streaming.api.dto.AccountMetricsResponse;
import com.demo.streaming.api.dto.ApiResponse;
import com.demo.streaming.api.dto.ErrorResponse;
import com.demo.streaming.api.service.AccountMetricsService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/accounts")
@Tag(name = "Account Real-Time Metrics", description = "Endpoints providing 1-minute tumbling window aggregated spend metrics")
public class AccountMetricsController {

    private final AccountMetricsService service;

    public AccountMetricsController(AccountMetricsService service) {
        this.service = service;
    }

    @GetMapping("/{accountId}/metrics")
    @Operation(
            summary = "Fetch rolling metrics for an account",
            description = "Returns the most recent 1-minute tumbling window metrics computed by the Kappa stream processor."
    )
    @ApiResponses({
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                    responseCode = "200",
                    description = "Metrics retrieved successfully",
                    content = @Content(schema = @Schema(implementation = ApiResponse.class))
            ),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                    responseCode = "400",
                    description = "Invalid request parameter",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))
            ),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                    responseCode = "500",
                    description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))
            )
    })
    public ResponseEntity<ApiResponse<List<AccountMetricsResponse>>> getAccountMetrics(
            @Parameter(description = "Customer account ID (e.g., ACC-1002)", example = "ACC-1002", required = true)
            @PathVariable String accountId,
            @Parameter(description = "Maximum window entries to return (1-100)", example = "20")
            @RequestParam(defaultValue = "20") int limit) {

        List<AccountMetricsResponse> metrics = service.getRecentAccountMetrics(accountId, limit);
        return ResponseEntity.ok(ApiResponse.success(metrics, "Account metrics retrieved successfully"));
    }
}

