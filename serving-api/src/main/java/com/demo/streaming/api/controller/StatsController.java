package com.demo.streaming.api.controller;

import com.demo.streaming.api.dto.ApiResponse;
import com.demo.streaming.api.dto.ErrorResponse;
import com.demo.streaming.api.dto.OverallStatsResponse;
import com.demo.streaming.api.service.StatsService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/stats")
@Tag(name = "Platform Analytics", description = "High-level summary of aggregated events, volume, and active alerts")
public class StatsController {

    private final StatsService service;

    public StatsController(StatsService service) {
        this.service = service;
    }

    @GetMapping
    @Operation(summary = "Get overall platform metrics", description = "Aggregates overall volume, transaction count, active accounts, and unresolved incidents.")
    @ApiResponses({
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                    responseCode = "200",
                    description = "Summary retrieved successfully",
                    content = @Content(schema = @Schema(implementation = ApiResponse.class))
            ),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                    responseCode = "500",
                    description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))
            )
    })
    public ResponseEntity<ApiResponse<OverallStatsResponse>> getSummary() {
        OverallStatsResponse summary = service.getSystemSummary();
        return ResponseEntity.ok(ApiResponse.success(summary, "Platform summary retrieved successfully"));
    }
}

