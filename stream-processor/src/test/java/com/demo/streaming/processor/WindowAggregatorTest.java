package com.demo.streaming.processor;

import com.demo.streaming.contracts.AccountMetricsWindow;
import com.demo.streaming.contracts.PaymentTransaction;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class WindowAggregatorTest {

    @Test
    @DisplayName("WindowAggregator correctly rolls up count, total, and max amount within the same window")
    void testTumblingWindowRollup() {
        WindowAggregator aggregator = new WindowAggregator(60);
        Instant fixedTime = Instant.parse("2026-11-02T15:00:10Z");

        PaymentTransaction tx1 = new PaymentTransaction(
                "tx-1", "ACC-TEST", "M1", BigDecimal.valueOf(100.00), "USD", null, "FP", fixedTime
        );
        PaymentTransaction tx2 = new PaymentTransaction(
                "tx-2", "ACC-TEST", "M2", BigDecimal.valueOf(250.00), "USD", null, "FP", fixedTime.plusSeconds(5)
        );
        PaymentTransaction tx3 = new PaymentTransaction(
                "tx-3", "ACC-TEST", "M3", BigDecimal.valueOf(50.00), "USD", null, "FP", fixedTime.plusSeconds(15)
        );

        aggregator.process(tx1);
        aggregator.process(tx2);
        AccountMetricsWindow finalWindow = aggregator.process(tx3);

        assertThat(finalWindow.accountId()).isEqualTo("ACC-TEST");
        assertThat(finalWindow.transactionCount()).isEqualTo(3);
        assertThat(finalWindow.totalAmount()).isEqualByComparingTo("400.00");
        assertThat(finalWindow.maxAmount()).isEqualByComparingTo("250.00");
    }
}

