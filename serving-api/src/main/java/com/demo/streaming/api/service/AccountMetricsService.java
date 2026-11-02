package com.demo.streaming.api.service;

import com.demo.streaming.api.dto.AccountMetricsResponse;

import java.util.List;

/**
 * Service orchestrating query operations on account metrics.
 */
public interface AccountMetricsService {
    List<AccountMetricsResponse> getRecentAccountMetrics(String accountId, int limit);
}

