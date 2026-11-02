package com.demo.streaming.api.websocket;

import com.demo.streaming.api.dto.FraudAlertResponse;
import com.demo.streaming.api.dto.OverallStatsResponse;
import com.demo.streaming.api.service.FraudAlertService;
import com.demo.streaming.api.service.StatsService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * Unit tests verifying live stream publishing and scheduling logic.
 */
@ExtendWith(MockitoExtension.class)
class LiveStreamPublisherTest {

    @Mock
    private StreamingEventWebSocketHandler webSocketHandler;

    @Mock
    private StatsService statsService;

    @Mock
    private FraudAlertService fraudAlertService;

    @InjectMocks
    private LiveStreamPublisher publisher;

    @Test
    @DisplayName("publishLiveStreamUpdates skips broadcasting when no active sessions")
    void publishSkipsWhenNoActiveSessions() {
        when(webSocketHandler.getActiveSessionCount()).thenReturn(0);

        publisher.publishLiveStreamUpdates();

        verifyNoInteractions(statsService);
        verifyNoInteractions(fraudAlertService);
        verify(webSocketHandler, never()).broadcast(anyString(), any());
    }

    @Test
    @DisplayName("publishLiveStreamUpdates broadcasts stats and alerts when active sessions exist")
    void publishBroadcastsWhenSessionsPresent() {
        when(webSocketHandler.getActiveSessionCount()).thenReturn(3);

        OverallStatsResponse mockStats = new OverallStatsResponse(
                5L, 120L, new BigDecimal("45000.00"), 2L
        );
        when(statsService.getSystemSummary()).thenReturn(mockStats);

        FraudAlertResponse mockAlert = new FraudAlertResponse(
                "alert-1", "tx-1", "ACC-1", "RULE_LARGE_AMOUNT_SPIKE", "CRITICAL",
                "Large spike", Collections.emptyMap(), Instant.now(), false
        );
        when(fraudAlertService.getAlerts(false, null, 10)).thenReturn(List.of(mockAlert));

        publisher.publishLiveStreamUpdates();

        verify(webSocketHandler).broadcast("STATS_UPDATE", mockStats);
        verify(webSocketHandler).broadcast("ALERTS_UPDATE", List.of(mockAlert));
    }
}
