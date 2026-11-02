package com.demo.streaming.api.service;

import com.demo.streaming.api.dto.FraudAlertResponse;
import com.demo.streaming.api.dto.ResolveAlertRequest;
import com.demo.streaming.api.exception.BusinessRuleViolationException;
import com.demo.streaming.api.exception.ResourceNotFoundException;
import com.demo.streaming.api.repository.FraudAlertRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FraudAlertServiceTest {

    @Mock
    private FraudAlertRepository repository;

    @InjectMocks
    private FraudAlertServiceImpl service;

    @Test
    @DisplayName("Resolving an active alert delegates to repository successfully")
    void resolveActiveAlertSuccess() {
        FraudAlertResponse unresolved = new FraudAlertResponse(
                "alert-101", "tx-1", "ACC-1", "RULE_LARGE_AMOUNT_SPIKE", "CRITICAL",
                "Large amount spike", Collections.emptyMap(), Instant.now(), false
        );

        when(repository.findById("alert-101")).thenReturn(Optional.of(unresolved));
        when(repository.markResolved(eq("alert-101"), anyString(), anyString())).thenReturn(true);

        ResolveAlertRequest request = new ResolveAlertRequest("Valid note verified with customer", "agent_1");
        service.resolveAlert("alert-101", request);

        verify(repository).markResolved("alert-101", "Valid note verified with customer", "agent_1");
    }

    @Test
    @DisplayName("Resolving already resolved alert throws BusinessRuleViolationException")
    void resolveAlreadyResolvedThrows() {
        FraudAlertResponse alreadyResolved = new FraudAlertResponse(
                "alert-102", "tx-2", "ACC-2", "RULE_HIGH_VELOCITY_BURST", "HIGH",
                "Velocity burst", Collections.emptyMap(), Instant.now(), true
        );

        when(repository.findById("alert-102")).thenReturn(Optional.of(alreadyResolved));

        ResolveAlertRequest request = new ResolveAlertRequest("Valid note", "agent_1");

        assertThatThrownBy(() -> service.resolveAlert("alert-102", request))
                .isInstanceOf(BusinessRuleViolationException.class)
                .hasMessageContaining("already marked as resolved");

        verify(repository, never()).markResolved(anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("Resolving non-existent alert throws ResourceNotFoundException")
    void resolveNonExistentThrows() {
        when(repository.findById("missing-alert")).thenReturn(Optional.empty());

        ResolveAlertRequest request = new ResolveAlertRequest("Valid note", "agent_1");

        assertThatThrownBy(() -> service.resolveAlert("missing-alert", request))
                .isInstanceOf(ResourceNotFoundException.class)
                .hasMessageContaining("Fraud alert not found");
    }
}

