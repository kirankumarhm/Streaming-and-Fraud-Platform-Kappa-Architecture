package com.demo.streaming.lambda;

import com.demo.streaming.contracts.PaymentTransaction;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;

/**
 * Batch layer aggregation engine that re-computes historical rollups with 100% precision.
 * Unlike the speed layer which operates over tumbling streaming windows, the batch layer
 * groups by date and account over the entire immutable master dataset.
 */
public class BatchAggregator {

    public record BatchAccountKey(String accountId, LocalDate batchDate) {}

    public static class BatchRollup {
        private final String accountId;
        private final LocalDate batchDate;
        private int transactionCount = 0;
        private BigDecimal totalAmount = BigDecimal.ZERO;
        private BigDecimal maxAmount = BigDecimal.ZERO;
        private String currency = "USD";

        public BatchRollup(String accountId, LocalDate batchDate) {
            this.accountId = accountId;
            this.batchDate = batchDate;
        }

        public synchronized void aggregate(BigDecimal amount, String curr) {
            this.transactionCount++;
            this.totalAmount = this.totalAmount.add(amount);
            if (amount.compareTo(this.maxAmount) > 0) {
                this.maxAmount = amount;
            }
            this.currency = curr;
        }

        public String getAccountId() { return accountId; }
        public LocalDate getBatchDate() { return batchDate; }
        public int getTransactionCount() { return transactionCount; }
        public BigDecimal getTotalAmount() { return totalAmount; }
        public BigDecimal getMaxAmount() { return maxAmount; }
        public String getCurrency() { return currency; }
    }

    private final Map<BatchAccountKey, BatchRollup> batchStore = new HashMap<>();

    public void process(PaymentTransaction tx) {
        LocalDate date = tx.timestamp().atZone(ZoneOffset.UTC).toLocalDate();
        BatchAccountKey key = new BatchAccountKey(tx.accountId(), date);
        BatchRollup rollup = batchStore.computeIfAbsent(key, k -> new BatchRollup(tx.accountId(), date));
        rollup.aggregate(tx.amount(), tx.currency());
    }

    public Collection<BatchRollup> getRollups() {
        return Collections.unmodifiableCollection(batchStore.values());
    }

    public void clear() {
        batchStore.clear();
    }
}
