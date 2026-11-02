package com.demo.streaming.api.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;

/**
 * Generic API response envelope standardizing successful payload distribution.
 */
@Schema(description = "Standard API Response Envelope")
public record ApiResponse<T>(
        @Schema(description = "Success flag indicator", example = "true")
        boolean success,

        @Schema(description = "Human-readable status or summary message", example = "Operation completed successfully")
        String message,

        @Schema(description = "Enclosed response payload")
        T data,

        @Schema(description = "UTC response generation timestamp")
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        Instant timestamp
) {
    public static <T> ApiResponse<T> success(T data, String message) {
        return new ApiResponse<>(true, message, data, Instant.now());
    }

    public static <T> ApiResponse<T> success(T data) {
        return success(data, "Success");
    }
}

