package com.demo.streaming.api.service;

import com.demo.streaming.api.dto.FraudAlertResponse;
import com.demo.streaming.api.dto.ResolveAlertRequest;
import com.demo.streaming.api.exception.BusinessRuleViolationException;
import com.demo.streaming.api.exception.ResourceNotFoundException;
import com.demo.streaming.api.repository.FraudAlertRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Objects;

@Service
public class FraudAlertServiceImpl implements FraudAlertService {

    private static final Logger log = LoggerFactory.getLogger(FraudAlertServiceImpl.class);
    private final FraudAlertRepository repository;

    public FraudAlertServiceImpl(FraudAlertRepository repository) {
        this.repository = repository;
    }

    @Override
    @Transactional(readOnly = true)
    public List<FraudAlertResponse> getAlerts(boolean includeResolved, String severity, int limit) {
        int boundedLimit = Math.clamp(limit, 1, 200);
        return repository.findAlerts(includeResolved, severity, boundedLimit);
    }

    @Override
    @Transactional(readOnly = true)
    public FraudAlertResponse getAlertById(String alertId) {
        Objects.requireNonNull(alertId, "alertId cannot be null");
        return repository.findById(alertId)
                .orElseThrow(() -> new ResourceNotFoundException("Fraud alert not found with ID: " + alertId));
    }

    @Override
    @Transactional
    public void resolveAlert(String alertId, ResolveAlertRequest request) {
        Objects.requireNonNull(alertId, "alertId cannot be null");
        Objects.requireNonNull(request, "request cannot be null");

        FraudAlertResponse existing = getAlertById(alertId);
        if (existing.resolved()) {
            throw new BusinessRuleViolationException("Fraud alert " + alertId + " is already marked as resolved.");
        }

        boolean updated = repository.markResolved(alertId, request.note(), request.resolvedBy());
        if (!updated) {
            throw new BusinessRuleViolationException("Failed to update resolution status for alert " + alertId);
        }

        log.info("Fraud alert {} resolved by {} with note: {}", alertId, request.resolvedBy(), request.note());
    }
}

