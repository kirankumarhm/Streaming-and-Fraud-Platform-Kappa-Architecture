package com.demo.streaming.api.service;

import com.demo.streaming.api.dto.FraudAlertResponse;
import com.demo.streaming.api.dto.ResolveAlertRequest;

import java.util.List;

/**
 * Service managing fraud incident retrieval and resolution lifecycle.
 */
public interface FraudAlertService {
    List<FraudAlertResponse> getAlerts(boolean includeResolved, String severity, int limit);
    FraudAlertResponse getAlertById(String alertId);
    void resolveAlert(String alertId, ResolveAlertRequest request);
}

