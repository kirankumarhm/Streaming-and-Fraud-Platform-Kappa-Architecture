package com.demo.streaming.api.service;

import com.demo.streaming.api.dto.OverallStatsResponse;

/**
 * Service providing high-level system summary metrics.
 */
public interface StatsService {
    OverallStatsResponse getSystemSummary();
}

