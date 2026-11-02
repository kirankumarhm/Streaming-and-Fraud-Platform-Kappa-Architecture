package com.demo.streaming.api.service;

import com.demo.streaming.api.dto.OverallStatsResponse;
import com.demo.streaming.api.repository.StatsRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional(readOnly = true)
public class StatsServiceImpl implements StatsService {

    private final StatsRepository repository;

    public StatsServiceImpl(StatsRepository repository) {
        this.repository = repository;
    }

    @Override
    public OverallStatsResponse getSystemSummary() {
        return repository.getSystemSummary();
    }
}

