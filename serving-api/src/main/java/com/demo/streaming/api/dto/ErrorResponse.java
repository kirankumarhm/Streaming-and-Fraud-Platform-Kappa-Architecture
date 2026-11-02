package com.demo.streaming.api.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.List;

/**
 * Standardized Problem Detail response conforming to RFC 7807 and RFC 9457.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description = "RFC 7807 / RFC 9457 Standard Problem Detail Error Representation")
public record ErrorResponse(
        @Schema(description = "HTTP Status Code", example = "400")
        int status,

        @Schema(description = "Short error type title", example = "Bad Request")
        String title,

        @Schema(description = "Application specific machine-readable error code", example = "VALIDATION_FAILED")
        String errorCode,

        @Schema(description = "Detailed human-readable error description", example = "Validation failed for 1 field")
        String detail,

        @Schema(description = "Request URI path where error occurred", example = "/api/v1/fraud/alerts")
        String path,

        @Schema(description = "List of invalid fields for validation failures")
        List<FieldErrorItem> fieldErrors,

        @Schema(description = "UTC occurrence timestamp")
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        Instant timestamp
) {
    public static ErrorResponse of(int status, String title, String errorCode, String detail, String path) {
        return new ErrorResponse(status, title, errorCode, detail, path, null, Instant.now());
    }

    public static ErrorResponse ofValidation(int status, String title, String errorCode, String detail, String path, List<FieldErrorItem> fieldErrors) {
        return new ErrorResponse(status, title, errorCode, detail, path, fieldErrors, Instant.now());
    }

    public record FieldErrorItem(String field, Object rejectedValue, String message) {}
}

