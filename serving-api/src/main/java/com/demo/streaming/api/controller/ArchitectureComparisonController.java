package com.demo.streaming.api.controller;

import com.demo.streaming.api.dto.ApiResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * Provides an analytical side-by-side architectural comparison of Kappa vs Lambda architectures.
 */
@RestController
@RequestMapping("/api/v1/comparison")
@Tag(name = "Architecture Comparison", description = "Analytical comparison between Kappa and Lambda architectural paradigms")
public class ArchitectureComparisonController {

    @GetMapping
    @Operation(summary = "Compare Kappa vs Lambda Architecture", description = "Returns architectural comparison metrics and design trade-offs.")
    public ApiResponse<Map<String, Object>> getComparison() {
        List<Map<String, String>> comparisonMatrix = List.of(
                Map.of(
                        "dimension", "Core Philosophy",
                        "lambda", "Batch for accuracy, streaming for low latency",
                        "kappa", "Everything is an infinite event stream"
                ),
                Map.of(
                        "dimension", "Pipelines",
                        "lambda", "2 Parallel Pipelines (Batch Layer + Speed Layer)",
                        "kappa", "1 Unified Pipeline (Stream Only)"
                ),
                Map.of(
                        "dimension", "Codebase Overhead",
                        "lambda", "Dual codebases (must maintain batch and streaming logic)",
                        "kappa", "Single codebase (logic written once in stream engine)"
                ),
                Map.of(
                        "dimension", "Serving Layer Query",
                        "lambda", "Query = Merge(Batch View, Speed View)",
                        "kappa", "Query = Direct read from stream-updated Serving View"
                ),
                Map.of(
                        "dimension", "Historical Reprocessing",
                        "lambda", "Run separate batch jobs over raw archives",
                        "kappa", "Rewind stream consumer group offset to 0 (--replay)"
                ),
                Map.of(
                        "dimension", "Synchronization Bugs",
                        "lambda", "High risk (divergent batch vs stream results)",
                        "kappa", "None (single business logic pipeline)"
                )
        );

        Map<String, Object> response = Map.of(
                "title", "Kappa vs. Lambda Architecture in Large-Scale Data Systems",
                "referenceArticle", "https://medium.com/@natesh.somanna/kappa-vs-lambda-architecture-in-large-scale-data-processing-systems-1da30973591d",
                "summary", "Lambda architecture uses dual pipelines to balance accuracy and speed. Kappa architecture simplifies operations by treating all data as streams with offset replay.",
                "matrix", comparisonMatrix
        );

        return ApiResponse.success(response, "Comparison matrix retrieved successfully");
    }
}

