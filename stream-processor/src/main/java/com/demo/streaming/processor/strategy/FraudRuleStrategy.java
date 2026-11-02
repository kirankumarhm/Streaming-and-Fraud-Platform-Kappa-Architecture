package com.demo.streaming.processor.strategy;

import com.demo.streaming.contracts.FraudAlert;
import com.demo.streaming.contracts.PaymentTransaction;

import java.util.Deque;
import java.util.Optional;

/**
 * Strategy interface defining contracts for modular, pluggable fraud detection algorithms.
 */
public interface FraudRuleStrategy {

    /**
     * Unique identifier for this rule.
     */
    String getRuleName();

    /**
     * Default severity classification assigned when this rule triggers.
     */
    FraudAlert.Severity getDefaultSeverity();

    /**
     * Evaluates a candidate transaction against historical state.
     *
     * @param current The incoming payment event being processed.
     * @param history Prior transactions recorded for this account within the sliding retention window.
     * @return An Optional containing the detected {@link FraudAlert}, or {@link Optional#empty()} if compliant.
     */
    Optional<FraudAlert> evaluate(PaymentTransaction current, Deque<PaymentTransaction> history);
}

