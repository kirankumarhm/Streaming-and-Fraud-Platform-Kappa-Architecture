package com.demo.streaming.api.controller;

import com.demo.streaming.api.dto.ApiResponse;
import com.demo.streaming.api.dto.ErrorResponse;
import com.demo.streaming.api.dto.FraudAlertResponse;
import com.demo.streaming.api.dto.ResolveAlertRequest;
import com.demo.streaming.api.service.FraudAlertService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/fraud")
@Tag(name = "Fraud Incident Management", description = "Endpoints to triage, inspect, and resolve real-time anomaly alerts")
public class FraudAlertController {

    private final FraudAlertService service;

    public FraudAlertController(FraudAlertService service) {
        this.service = service;
    }

    @GetMapping("/alerts")
    @Operation(
            summary = "Query fraud alerts",
            description = "Retrieves flagged anomaly incidents filtered by resolution status and severity tier."
    )
    @ApiResponses({
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                    responseCode = "200",
                    description = "Alerts returned successfully",
                    content = @Content(schema = @Schema(implementation = ApiResponse.class))
            ),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                    responseCode = "500",
                    description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))
            )
    })
    public ResponseEntity<ApiResponse<List<FraudAlertResponse>>> getAlerts(
            @Parameter(description = "Include resolved alerts", example = "false")
            @RequestParam(defaultValue = "false") boolean includeResolved,
            @Parameter(description = "Filter by severity (LOW, MEDIUM, HIGH, CRITICAL)", example = "CRITICAL")
            @RequestParam(required = false) String severity,
            @Parameter(description = "Maximum results to fetch", example = "50")
            @RequestParam(defaultValue = "50") int limit) {

        List<FraudAlertResponse> alerts = service.getAlerts(includeResolved, severity, limit);
        return ResponseEntity.ok(ApiResponse.success(alerts, "Fraud alerts retrieved successfully"));
    }

    @GetMapping("/alerts/{alertId}")
    @Operation(summary = "Get fraud alert by ID", description = "Fetches complete forensic details and metadata for a specific alert.")
    @ApiResponses({
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                    responseCode = "200",
                    description = "Alert found",
                    content = @Content(schema = @Schema(implementation = ApiResponse.class))
            ),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                    responseCode = "404",
                    description = "Alert not found",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))
            )
    })
    public ResponseEntity<ApiResponse<FraudAlertResponse>> getAlertById(
            @Parameter(description = "Alert unique UUID", required = true)
            @PathVariable String alertId) {
        FraudAlertResponse alert = service.getAlertById(alertId);
        return ResponseEntity.ok(ApiResponse.success(alert, "Fraud alert found"));
    }

    @PostMapping("/alerts/{alertId}/resolve")
    @Operation(summary = "Resolve a fraud alert", description = "Marks an active fraud incident as resolved with triage notes.")
    @ApiResponses({
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                    responseCode = "200",
                    description = "Alert resolved successfully",
                    content = @Content(schema = @Schema(implementation = ApiResponse.class))
            ),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                    responseCode = "400",
                    description = "Validation failed on payload",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))
            ),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                    responseCode = "404",
                    description = "Alert does not exist",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))
            ),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                    responseCode = "422",
                    description = "Alert is already resolved",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))
            )
    })
    public ResponseEntity<ApiResponse<Void>> resolveAlert(
            @Parameter(description = "Alert unique UUID", required = true)
            @PathVariable String alertId,
            @Valid @RequestBody ResolveAlertRequest request) {

        service.resolveAlert(alertId, request);
        return ResponseEntity.ok(ApiResponse.success(null, "Fraud alert resolved successfully"));
    }
}

