package com.demo.streaming.processor.strategy;

import com.demo.streaming.contracts.FraudAlert;
import com.demo.streaming.contracts.PaymentTransaction;

import java.math.BigDecimal;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Strategy implementation detecting abnormal monetary transaction spikes exceeding risk thresholds.
 */
public class LargeAmountSpikeRuleStrategy implements FraudRuleStrategy {

    private static final String RULE_NAME = "RULE_LARGE_AMOUNT_SPIKE";
    private final BigDecimal threshold;

    public LargeAmountSpikeRuleStrategy(BigDecimal threshold) {
        this.threshold = threshold;
    }

    public LargeAmountSpikeRuleStrategy() {
        this(BigDecimal.valueOf(10000.00));
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
        if (current.amount().compareTo(threshold) >= 0) {
            FraudAlert alert = FraudAlert.createUnresolved(
                    UUID.randomUUID().toString(),
                    current.transactionId(),
                    current.accountId(),
                    getRuleName(),
                    getDefaultSeverity(),
                    String.format("Transaction amount $%s exceeded risk ceiling of $%s", current.amount(), threshold),
                    Map.of(
                            "observedAmount", current.amount(),
                            "thresholdAmount", threshold,
                            "currency", current.currency()
                    ),
                    current.timestamp()
            );
            return Optional.of(alert);
        }
        return Optional.empty();
    }
}

