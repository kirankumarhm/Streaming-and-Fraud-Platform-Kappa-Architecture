package com.demo.streaming.contracts;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/**
 * Immutable canonical event representing a financial payment transaction.
 * Adheres to enterprise boundary validation standards with defensive checks.
 *
 * @param transactionId Unique UUID identifying the transaction.
 * @param accountId     Originating customer account identifier.
 * @param merchantId    Target merchant or beneficiary identifier.
 * @param amount        Monetary value of the transaction (must be positive).
 * @param currency      ISO 4217 standard three-letter currency code (e.g., USD, EUR).
 * @param location      Physical or geo-ip coordinates where the event was initiated.
 * @param deviceFingerprint Unique hardware/browser signature of the client device.
 * @param timestamp     Exact UTC instant when the transaction was authorized.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record PaymentTransaction(
        @NotBlank(message = "Transaction ID must not be blank")
        String transactionId,

        @NotBlank(message = "Account ID must not be blank")
        String accountId,

        @NotBlank(message = "Merchant ID must not be blank")
        String merchantId,

        @NotNull(message = "Amount is mandatory")
        @DecimalMin(value = "0.01", message = "Amount must be greater than zero")
        BigDecimal amount,

        @NotBlank(message = "Currency code is required")
        @Pattern(regexp = "^[A-Z]{3}$", message = "Currency must be a valid 3-letter ISO code")
        String currency,

        @Valid
        Location location,

        @NotBlank(message = "Device fingerprint is required for fraud scoring")
        String deviceFingerprint,

        @NotNull(message = "Timestamp is mandatory")
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        Instant timestamp
) {
    /**
     * Compact constructor enforcing defensive non-null checks and standard formatting.
     */
    public PaymentTransaction {
        Objects.requireNonNull(transactionId, "transactionId cannot be null");
        Objects.requireNonNull(accountId, "accountId cannot be null");
        Objects.requireNonNull(merchantId, "merchantId cannot be null");
        Objects.requireNonNull(amount, "amount cannot be null");
        Objects.requireNonNull(currency, "currency cannot be null");
        Objects.requireNonNull(deviceFingerprint, "deviceFingerprint cannot be null");
        Objects.requireNonNull(timestamp, "timestamp cannot be null");

        // Normalize currency code to uppercase
        currency = currency.trim().toUpperCase();
    }

    /**
     * Represents geographical coordinates for fraud velocity and distance calculations.
     */
    public record Location(
            @NotBlank(message = "City is mandatory")
            String city,

            @NotBlank(message = "Country is mandatory")
            String country,

            double latitude,
            double longitude
    ) {
        public Location {
            if (latitude < -90.0 || latitude > 90.0) {
                throw new IllegalArgumentException("Latitude must be between -90 and 90. Received: " + latitude);
            }
            if (longitude < -180.0 || longitude > 180.0) {
                throw new IllegalArgumentException("Longitude must be between -180 and 180. Received: " + longitude);
            }
        }
    }
}
