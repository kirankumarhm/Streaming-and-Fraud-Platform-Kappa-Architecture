package com.demo.streaming.api.repository;

import com.demo.streaming.api.dto.AccountMetricsResponse;

import java.util.List;

/**
 * Repository interface abstracting persistence operations for account window aggregates.
 */
public interface AccountMetricsRepository {
    List<AccountMetricsResponse> findLatestMetricsByAccountId(String accountId, int limit);
}

