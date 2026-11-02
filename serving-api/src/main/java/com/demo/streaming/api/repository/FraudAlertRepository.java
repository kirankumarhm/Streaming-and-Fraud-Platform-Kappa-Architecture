package com.demo.streaming.api.repository;

import com.demo.streaming.api.dto.FraudAlertResponse;

import java.util.List;
import java.util.Optional;

/**
 * Repository interface for managing fraud alert persistence and triage state.
 */
public interface FraudAlertRepository {
    List<FraudAlertResponse> findAlerts(boolean includeResolved, String severity, int limit);
    Optional<FraudAlertResponse> findById(String alertId);
    boolean markResolved(String alertId, String note, String resolvedBy);
}

