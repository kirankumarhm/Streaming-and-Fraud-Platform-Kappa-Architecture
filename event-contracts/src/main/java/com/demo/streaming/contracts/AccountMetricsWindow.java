package com.demo.streaming.contracts;

import com.fasterxml.jackson.annotation.JsonFormat;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/**
 * Immutable aggregated metrics snapshot across a fixed time window.
 *
 * @param accountId        Customer account identifier.
 * @param windowStart      Window lower bound (inclusive).
 * @param windowEnd        Window upper bound (exclusive).
 * @param transactionCount Number of events recorded inside the window.
 * @param totalAmount      Cumulative transaction volume in window currency.
 * @param maxAmount        Maximum single transaction value observed.
 * @param currency         Operating currency of the rollup.
 */
public record AccountMetricsWindow(
        @NotBlank String accountId,
        @NotNull @JsonFormat(shape = JsonFormat.Shape.STRING) Instant windowStart,
        @NotNull @JsonFormat(shape = JsonFormat.Shape.STRING) Instant windowEnd,
        @PositiveOrZero int transactionCount,
        @NotNull @DecimalMin("0.00") BigDecimal totalAmount,
        @NotNull @DecimalMin("0.00") BigDecimal maxAmount,
        @NotBlank String currency
) {
    public AccountMetricsWindow {
        Objects.requireNonNull(accountId, "accountId cannot be null");
        Objects.requireNonNull(windowStart, "windowStart cannot be null");
        Objects.requireNonNull(windowEnd, "windowEnd cannot be null");
        Objects.requireNonNull(totalAmount, "totalAmount cannot be null");
        Objects.requireNonNull(maxAmount, "maxAmount cannot be null");
        Objects.requireNonNull(currency, "currency cannot be null");

        if (windowEnd.isBefore(windowStart)) {
            throw new IllegalArgumentException("windowEnd must not be before windowStart");
        }
    }
}
