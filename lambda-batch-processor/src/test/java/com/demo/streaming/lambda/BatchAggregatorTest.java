package com.demo.streaming.lambda;

import com.demo.streaming.contracts.PaymentTransaction;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

class BatchAggregatorTest {

    @Test
    @DisplayName("BatchAggregator rolls up multiple transactions on the same day into a single authoritative master view")
    void testBatchDailyRollup() {
        BatchAggregator aggregator = new BatchAggregator();

        Instant day1Time1 = Instant.parse("2026-11-02T10:15:00Z");
        Instant day1Time2 = Instant.parse("2026-11-02T16:30:00Z");
        Instant day2Time1 = Instant.parse("2026-11-03T09:00:00Z");

        aggregator.process(new PaymentTransaction("t1", "ACC-1", "M1", BigDecimal.valueOf(100.00), "USD", null, "FP", day1Time1));
        aggregator.process(new PaymentTransaction("t2", "ACC-1", "M2", BigDecimal.valueOf(250.00), "USD", null, "FP", day1Time2));
        aggregator.process(new PaymentTransaction("t3", "ACC-1", "M3", BigDecimal.valueOf(75.00), "USD", null, "FP", day2Time1));

        Collection<BatchAggregator.BatchRollup> rollups = aggregator.getRollups();
        assertThat(rollups).hasSize(2);

        BatchAggregator.BatchRollup nov2 = rollups.stream()
                .filter(r -> r.getBatchDate().equals(LocalDate.parse("2026-11-02")))
                .findFirst().orElseThrow();

        assertThat(nov2.getTransactionCount()).isEqualTo(2);
        assertThat(nov2.getTotalAmount()).isEqualByComparingTo("350.00");
        assertThat(nov2.getMaxAmount()).isEqualByComparingTo("250.00");

        BatchAggregator.BatchRollup nov3 = rollups.stream()
                .filter(r -> r.getBatchDate().equals(LocalDate.parse("2026-11-03")))
                .findFirst().orElseThrow();

        assertThat(nov3.getTransactionCount()).isEqualTo(1);
        assertThat(nov3.getTotalAmount()).isEqualByComparingTo("75.00");
    }
}

