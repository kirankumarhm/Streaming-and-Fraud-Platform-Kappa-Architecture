package com.demo.streaming.processor.strategy;

import com.demo.streaming.contracts.FraudAlert;
import com.demo.streaming.contracts.PaymentTransaction;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Registry and orchestrator for active {@link FraudRuleStrategy} instances.
 * Manages per-account sliding event history safely with TTL pruning.
 */
public class FraudRuleRegistry {

    private final List<FraudRuleStrategy> strategies = new CopyOnWriteArrayList<>();
    private final Map<String, Deque<PaymentTransaction>> accountState = new ConcurrentHashMap<>();
    private static final Duration STATE_RETENTION_TTL = Duration.ofMinutes(15);

    public FraudRuleRegistry() {
        // Register default production strategies
        register(new LargeAmountSpikeRuleStrategy());
        register(new HighVelocityBurstRuleStrategy());
        register(new ImpossibleTravelRuleStrategy());
    }

    public void register(FraudRuleStrategy strategy) {
        Objects.requireNonNull(strategy, "strategy cannot be null");
        this.strategies.add(strategy);
    }

    /**
     * Executes all registered strategies against the transaction and maintains keyed history.
     */
    public List<FraudAlert> evaluateAll(PaymentTransaction current) {
        Objects.requireNonNull(current, "transaction cannot be null");
        List<FraudAlert> detectedAlerts = new ArrayList<>();

        Deque<PaymentTransaction> history = accountState.computeIfAbsent(current.accountId(), k -> new ArrayDeque<>());
        synchronized (history) {
            // Prune expired historical entries
            Instant cutoff = current.timestamp().minus(STATE_RETENTION_TTL);
            while (!history.isEmpty() && history.peekFirst().timestamp().isBefore(cutoff)) {
                history.pollFirst();
            }

            // Run each strategy
            for (FraudRuleStrategy strategy : strategies) {
                strategy.evaluate(current, history).ifPresent(detectedAlerts::add);
            }

            // Append current transaction to history
            history.addLast(current);
        }

        return Collections.unmodifiableList(detectedAlerts);
    }

    /**
     * Periodically prunes accounts that have been inactive.
     */
    public void cleanupInactiveAccounts() {
        Instant cutoff = Instant.now().minus(STATE_RETENTION_TTL);
        accountState.entrySet().removeIf(entry -> {
            synchronized (entry.getValue()) {
                return entry.getValue().isEmpty() || entry.getValue().peekLast().timestamp().isBefore(cutoff);
            }
        });
    }
}

