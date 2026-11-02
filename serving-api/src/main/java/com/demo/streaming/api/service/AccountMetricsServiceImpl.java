package com.demo.streaming.api.service;

import com.demo.streaming.api.dto.AccountMetricsResponse;
import com.demo.streaming.api.repository.AccountMetricsRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Objects;

@Service
@Transactional(readOnly = true)
public class AccountMetricsServiceImpl implements AccountMetricsService {

    private final AccountMetricsRepository repository;

    public AccountMetricsServiceImpl(AccountMetricsRepository repository) {
        this.repository = repository;
    }

    @Override
    public List<AccountMetricsResponse> getRecentAccountMetrics(String accountId, int limit) {
        Objects.requireNonNull(accountId, "accountId cannot be null");
        int boundedLimit = Math.clamp(limit, 1, 100);
        return repository.findLatestMetricsByAccountId(accountId, boundedLimit);
    }
}

