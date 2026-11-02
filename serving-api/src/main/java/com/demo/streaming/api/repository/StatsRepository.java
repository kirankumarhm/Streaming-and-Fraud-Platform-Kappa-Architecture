package com.demo.streaming.api.repository;

import com.demo.streaming.api.dto.OverallStatsResponse;

/**
 * Repository interface for system analytics aggregations.
 */
public interface StatsRepository {
    OverallStatsResponse getSystemSummary();
}

