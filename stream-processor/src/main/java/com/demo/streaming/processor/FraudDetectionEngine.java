package com.demo.streaming.processor;

import com.demo.streaming.contracts.FraudAlert;
import com.demo.streaming.contracts.PaymentTransaction;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class FraudDetectionEngine {
    private static final BigDecimal SPIKE_THRESHOLD = BigDecimal.valueOf(10000.00);
    private static final int VELOCITY_COUNT_THRESHOLD = 3;
    private static final Duration VELOCITY_WINDOW = Duration.ofSeconds(10);
    private static final double IMPOSSIBLE_TRAVEL_KM = 1000.0;
    private static final Duration IMPOSSIBLE_TRAVEL_WINDOW = Duration.ofMinutes(5);

    // Keyed State per Account (In a multi-node Flink cluster, this is backed by RocksDB)
    private final Map<String, Deque<PaymentTransaction>> accountHistory = new ConcurrentHashMap<>();

    public List<FraudAlert> evaluate(PaymentTransaction tx) {
        List<FraudAlert> alerts = new ArrayList<>();
        Instant now = tx.timestamp();

        // Rule 1: High-Amount Spike
        if (tx.amount().compareTo(SPIKE_THRESHOLD) >= 0) {
            alerts.add(new FraudAlert(
                    UUID.randomUUID().toString(),
                    tx.transactionId(),
                    tx.accountId(),
                    "RULE_LARGE_AMOUNT_SPIKE",
                    FraudAlert.Severity.CRITICAL,
                    "Transaction amount $" + tx.amount() + " exceeded threshold $" + SPIKE_THRESHOLD,
                    Map.of("amount", tx.amount(), "threshold", SPIKE_THRESHOLD),
                    now,
                    false
            ));
        }

        Deque<PaymentTransaction> history = accountHistory.computeIfAbsent(tx.accountId(), k -> new ArrayDeque<>());
        synchronized (history) {
            // Prune history older than 10 minutes to prevent memory leak
            Instant pruneThreshold = now.minus(Duration.ofMinutes(10));
            while (!history.isEmpty() && history.peekFirst().timestamp().isBefore(pruneThreshold)) {
                history.pollFirst();
            }

            // Rule 2: Velocity Check (Multiple transactions in a short window)
            long recentCount = history.stream()
                    .filter(past -> Duration.between(past.timestamp(), now).compareTo(VELOCITY_WINDOW) <= 0)
                    .count();

            if (recentCount >= VELOCITY_COUNT_THRESHOLD) {
                alerts.add(new FraudAlert(
                        UUID.randomUUID().toString(),
                        tx.transactionId(),
                        tx.accountId(),
                        "RULE_HIGH_VELOCITY_BURST",
                        FraudAlert.Severity.HIGH,
                        String.format("Account recorded %d transactions within %d seconds", recentCount + 1, VELOCITY_WINDOW.toSeconds()),
                        Map.of("burstCount", recentCount + 1, "windowSeconds", VELOCITY_WINDOW.toSeconds()),
                        now,
                        false
                ));
            }

            // Rule 3: Impossible Geo-Travel Check
            if (!history.isEmpty() && tx.location() != null) {
                PaymentTransaction lastTx = history.peekLast();
                if (lastTx != null && lastTx.location() != null) {
                    Duration timeDiff = Duration.between(lastTx.timestamp(), now).abs();
                    if (timeDiff.compareTo(IMPOSSIBLE_TRAVEL_WINDOW) <= 0) {
                        double distanceKm = haversineKm(
                                lastTx.location().latitude(), lastTx.location().longitude(),
                                tx.location().latitude(), tx.location().longitude()
                        );
                        if (distanceKm > IMPOSSIBLE_TRAVEL_KM) {
                            alerts.add(new FraudAlert(
                                    UUID.randomUUID().toString(),
                                    tx.transactionId(),
                                    tx.accountId(),
                                    "RULE_IMPOSSIBLE_TRAVEL",
                                    FraudAlert.Severity.CRITICAL,
                                    String.format("Impossible travel detected: %.1f km in %d seconds (from %s to %s)",
                                            distanceKm, timeDiff.toSeconds(), lastTx.location().city(), tx.location().city()),
                                    Map.of("distanceKm", distanceKm, "timeSeconds", timeDiff.toSeconds(),
                                            "fromCity", lastTx.location().city(), "toCity", tx.location().city()),
                                    now,
                                    false
                            ));
                        }
                    }
                }
            }

            history.addLast(tx);
        }

        return alerts;
    }

    private double haversineKm(double lat1, double lon1, double lat2, double lon2) {
        final int R = 6371; // Earth radius in kilometers
        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }
}

