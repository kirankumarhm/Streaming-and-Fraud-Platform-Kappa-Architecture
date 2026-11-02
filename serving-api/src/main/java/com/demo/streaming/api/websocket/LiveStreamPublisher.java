package com.demo.streaming.api.websocket;

import com.demo.streaming.api.dto.FraudAlertResponse;
import com.demo.streaming.api.dto.OverallStatsResponse;
import com.demo.streaming.api.service.FraudAlertService;
import com.demo.streaming.api.service.StatsService;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Periodically polls the latest high-frequency stream state and broadcasts updates
 * over WebSocket to all active dashboard clients with sub-second latency.
 */
@Component
public class LiveStreamPublisher {

    private final StreamingEventWebSocketHandler webSocketHandler;
    private final StatsService statsService;
    private final FraudAlertService fraudAlertService;

    public LiveStreamPublisher(
            StreamingEventWebSocketHandler webSocketHandler,
            StatsService statsService,
            FraudAlertService fraudAlertService) {
        this.webSocketHandler = webSocketHandler;
        this.statsService = statsService;
        this.fraudAlertService = fraudAlertService;
    }

    @Scheduled(fixedDelay = 1000)
    public void publishLiveStreamUpdates() {
        if (webSocketHandler.getActiveSessionCount() == 0) {
            return;
        }

        // Push KPI stats update
        OverallStatsResponse stats = statsService.getSystemSummary();
        webSocketHandler.broadcast("STATS_UPDATE", stats);

        // Push latest unresolved fraud alerts
        List<FraudAlertResponse> alerts = fraudAlertService.getAlerts(false, null, 10);
        webSocketHandler.broadcast("ALERTS_UPDATE", alerts);
    }
}
