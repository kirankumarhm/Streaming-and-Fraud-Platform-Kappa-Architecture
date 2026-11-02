package com.demo.streaming.processor.strategy;

import com.demo.streaming.contracts.FraudAlert;
import com.demo.streaming.contracts.PaymentTransaction;
import com.demo.streaming.processor.util.GeoUtils;

import java.time.Duration;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Strategy identifying impossible geographical displacement (e.g. London to Tokyo in 2 minutes).
 */
public class ImpossibleTravelRuleStrategy implements FraudRuleStrategy {

    private static final String RULE_NAME = "RULE_IMPOSSIBLE_TRAVEL";
    private final double maxDistanceKm;
    private final Duration maxTimeDifference;

    public ImpossibleTravelRuleStrategy(double maxDistanceKm, Duration maxTimeDifference) {
        this.maxDistanceKm = maxDistanceKm;
        this.maxTimeDifference = maxTimeDifference;
    }

    public ImpossibleTravelRuleStrategy() {
        this(1000.0, Duration.ofMinutes(5));
    }

    @Override
    public String getRuleName() {
        return RULE_NAME;
    }

    @Override
    public FraudAlert.Severity getDefaultSeverity() {
        return FraudAlert.Severity.CRITICAL;
    }

    @Override
    public Optional<FraudAlert> evaluate(PaymentTransaction current, Deque<PaymentTransaction> history) {
        if (history == null || history.isEmpty() || current.location() == null) {
            return Optional.empty();
        }

        PaymentTransaction previous = history.peekLast();
        if (previous == null || previous.location() == null) {
            return Optional.empty();
        }

        Duration elapsed = Duration.between(previous.timestamp(), current.timestamp()).abs();
        if (elapsed.compareTo(maxTimeDifference) <= 0) {
            double distanceKm = GeoUtils.calculateHaversineDistanceKm(
                    previous.location().latitude(), previous.location().longitude(),
                    current.location().latitude(), current.location().longitude()
            );

            if (distanceKm > maxDistanceKm) {
                FraudAlert alert = FraudAlert.createUnresolved(
                        UUID.randomUUID().toString(),
                        current.transactionId(),
                        current.accountId(),
                        getRuleName(),
                        getDefaultSeverity(),
                        String.format("Impossible displacement: %.1f km traversed in %d seconds between %s and %s",
                                distanceKm, elapsed.toSeconds(), previous.location().city(), current.location().city()),
                        Map.of(
                                "distanceKm", distanceKm,
                                "elapsedSeconds", elapsed.toSeconds(),
                                "originCity", previous.location().city(),
                                "destinationCity", current.location().city()
                        ),
                        current.timestamp()
                );
                return Optional.of(alert);
            }
        }

        return Optional.empty();
    }
}

