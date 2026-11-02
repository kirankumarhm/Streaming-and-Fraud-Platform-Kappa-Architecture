package com.demo.streaming.processor.strategy;

import com.demo.streaming.contracts.FraudAlert;
import com.demo.streaming.contracts.PaymentTransaction;

import java.time.Duration;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Strategy implementation identifying automated credential stuffing or card testing (high velocity bursts).
 */
public class HighVelocityBurstRuleStrategy implements FraudRuleStrategy {

    private static final String RULE_NAME = "RULE_HIGH_VELOCITY_BURST";
    private final int burstCountThreshold;
    private final Duration windowDuration;

    public HighVelocityBurstRuleStrategy(int burstCountThreshold, Duration windowDuration) {
        this.burstCountThreshold = burstCountThreshold;
        this.windowDuration = windowDuration;
    }

    public HighVelocityBurstRuleStrategy() {
        this(3, Duration.ofSeconds(10));
    }

    @Override
    public String getRuleName() {
        return RULE_NAME;
    }

    @Override
    public FraudAlert.Severity getDefaultSeverity() {
        return FraudAlert.Severity.HIGH;
    }

    @Override
    public Optional<FraudAlert> evaluate(PaymentTransaction current, Deque<PaymentTransaction> history) {
        if (history == null || history.isEmpty()) {
            return Optional.empty();
        }

        long countInWindow = history.stream()
                .filter(past -> Duration.between(past.timestamp(), current.timestamp()).compareTo(windowDuration) <= 0)
                .count();

        if (countInWindow >= burstCountThreshold) {
            FraudAlert alert = FraudAlert.createUnresolved(
                    UUID.randomUUID().toString(),
                    current.transactionId(),
                    current.accountId(),
                    getRuleName(),
                    getDefaultSeverity(),
                    String.format("Account recorded %d transactions within %d seconds (Threshold: %d)",
                            countInWindow + 1, windowDuration.toSeconds(), burstCountThreshold),
                    Map.of(
                            "burstTransactionCount", countInWindow + 1,
                            "windowSeconds", windowDuration.toSeconds(),
                            "thresholdLimit", burstCountThreshold
                    ),
                    current.timestamp()
            );
            return Optional.of(alert);
        }
        return Optional.empty();
    }
}

