package com.demo.streaming.processor.strategy;

import com.demo.streaming.contracts.FraudAlert;
import com.demo.streaming.contracts.PaymentTransaction;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class FraudRuleStrategyTest {

    @Test
    @DisplayName("LargeAmountSpikeRuleStrategy fires when amount exceeds threshold")
    void testLargeAmountSpikeStrategy() {
        LargeAmountSpikeRuleStrategy strategy = new LargeAmountSpikeRuleStrategy(BigDecimal.valueOf(10000.00));

        PaymentTransaction normal = createTx("tx-1", "ACC-1", BigDecimal.valueOf(500.00), Instant.now(), null);
        Optional<FraudAlert> alert1 = strategy.evaluate(normal, new ArrayDeque<>());
        assertThat(alert1).isEmpty();

        PaymentTransaction spike = createTx("tx-2", "ACC-1", BigDecimal.valueOf(15000.00), Instant.now(), null);
        Optional<FraudAlert> alert2 = strategy.evaluate(spike, new ArrayDeque<>());
        assertThat(alert2).isPresent();
        assertThat(alert2.get().ruleName()).isEqualTo("RULE_LARGE_AMOUNT_SPIKE");
        assertThat(alert2.get().severity()).isEqualTo(FraudAlert.Severity.CRITICAL);
    }

    @Test
    @DisplayName("HighVelocityBurstRuleStrategy fires when rapid transactions occur within window")
    void testHighVelocityBurstStrategy() {
        HighVelocityBurstRuleStrategy strategy = new HighVelocityBurstRuleStrategy();
        Instant now = Instant.now();
        Deque<PaymentTransaction> history = new ArrayDeque<>();

        // Add 3 past transactions in the last 3 seconds
        history.add(createTx("tx-1", "ACC-2", BigDecimal.valueOf(10), now.minusSeconds(3), null));
        history.add(createTx("tx-2", "ACC-2", BigDecimal.valueOf(20), now.minusSeconds(2), null));
        history.add(createTx("tx-3", "ACC-2", BigDecimal.valueOf(30), now.minusSeconds(1), null));

        PaymentTransaction current = createTx("tx-4", "ACC-2", BigDecimal.valueOf(40), now, null);
        Optional<FraudAlert> alert = strategy.evaluate(current, history);

        assertThat(alert).isPresent();
        assertThat(alert.get().ruleName()).isEqualTo("RULE_HIGH_VELOCITY_BURST");
        assertThat(alert.get().severity()).isEqualTo(FraudAlert.Severity.HIGH);
    }

    @Test
    @DisplayName("ImpossibleTravelRuleStrategy fires on supersonic displacement between cities")
    void testImpossibleTravelStrategy() {
        ImpossibleTravelRuleStrategy strategy = new ImpossibleTravelRuleStrategy();
        Instant now = Instant.now();
        Deque<PaymentTransaction> history = new ArrayDeque<>();

        PaymentTransaction.Location nyc = new PaymentTransaction.Location("New York", "USA", 40.7128, -74.0060);
        PaymentTransaction.Location london = new PaymentTransaction.Location("London", "UK", 51.5074, -0.1278);

        // Previous transaction in NYC 30 seconds ago
        history.add(createTx("tx-1", "ACC-3", BigDecimal.valueOf(10), now.minusSeconds(30), nyc));

        // Current transaction in London right now
        PaymentTransaction current = createTx("tx-2", "ACC-3", BigDecimal.valueOf(50), now, london);
        Optional<FraudAlert> alert = strategy.evaluate(current, history);

        assertThat(alert).isPresent();
        assertThat(alert.get().ruleName()).isEqualTo("RULE_IMPOSSIBLE_TRAVEL");
        assertThat(alert.get().severity()).isEqualTo(FraudAlert.Severity.CRITICAL);
    }

    private PaymentTransaction createTx(String txId, String accId, BigDecimal amount, Instant time, PaymentTransaction.Location loc) {
        return new PaymentTransaction(txId, accId, "MERCHANT", amount, "USD", loc, "DEVICE-FP", time);
    }
}
