package com.demo.streaming.processor;

import com.demo.streaming.contracts.AccountMetricsWindow;
import com.demo.streaming.contracts.PaymentTransaction;
import com.demo.streaming.processor.util.DateTimeUtils;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stateful tumbling window aggregator computing rolling volume, counts, and maximum amounts.
 */
public class WindowAggregator {

    private static final long DEFAULT_WINDOW_SECONDS = 60L; // 1-minute tumbling window
    private final long windowDurationSeconds;

    private record WindowKey(String accountId, Instant windowStart) {}

    private static class AggregateBucket {
        final Instant windowStart;
        final Instant windowEnd;
        int count = 0;
        BigDecimal total = BigDecimal.ZERO;
        BigDecimal max = BigDecimal.ZERO;
        String currency = "USD";

        AggregateBucket(Instant start, Instant end) {
            this.windowStart = start;
            this.windowEnd = end;
        }

        synchronized void add(BigDecimal amount, String curr) {
            this.count++;
            this.total = this.total.add(amount);
            if (amount.compareTo(this.max) > 0) {
                this.max = amount;
            }
            this.currency = curr;
        }

        synchronized AccountMetricsWindow toSnapshot(String accountId) {
            return new AccountMetricsWindow(accountId, windowStart, windowEnd, count, total, max, currency);
        }
    }

    private final Map<WindowKey, AggregateBucket> activeWindows = new ConcurrentHashMap<>();

    public WindowAggregator(long windowDurationSeconds) {
        this.windowDurationSeconds = windowDurationSeconds;
    }

    public WindowAggregator() {
        this(DEFAULT_WINDOW_SECONDS);
    }

    /**
     * Ingests an event into its corresponding temporal window bucket and returns an updated snapshot.
     */
    public AccountMetricsWindow process(PaymentTransaction tx) {
        Objects.requireNonNull(tx, "transaction cannot be null");

        Instant windowStart = DateTimeUtils.floorToWindowStart(tx.timestamp(), windowDurationSeconds);
        Instant windowEnd = DateTimeUtils.calculateWindowEnd(windowStart, windowDurationSeconds);

        WindowKey key = new WindowKey(tx.accountId(), windowStart);
        AggregateBucket bucket = activeWindows.computeIfAbsent(key, k -> new AggregateBucket(windowStart, windowEnd));
        bucket.add(tx.amount(), tx.currency());

        return bucket.toSnapshot(tx.accountId());
    }

    /**
     * Purges expired windows from memory.
     */
    public void cleanupOldWindows() {
        Instant cutoff = Instant.now().minus(15, ChronoUnit.MINUTES);
        activeWindows.keySet().removeIf(k -> k.windowStart().isBefore(cutoff));
    }
}
