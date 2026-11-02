package com.demo.streaming.api.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Production WebSocket handler managing live dashboard streaming connections and broadcasting events.
 */
@Component
public class StreamingEventWebSocketHandler extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(StreamingEventWebSocketHandler.class);
    private final Set<WebSocketSession> sessions = ConcurrentHashMap.newKeySet();
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        sessions.add(session);
        log.info("WebSocket connection established. SessionId={}, Total Active Clients={}", session.getId(), sessions.size());
        sendDirect(session, "SYSTEM_CONNECTED", Map.of("sessionId", session.getId(), "message", "Connected to Real-Time Event Stream"));
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        sessions.remove(session);
        log.info("WebSocket connection closed. SessionId={}, Total Active Clients={}", session.getId(), sessions.size());
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        log.warn("WebSocket transport error on session {}: {}", session.getId(), exception.getMessage());
        sessions.remove(session);
    }

    /**
     * Broadcasts a typed real-time event to all connected dashboard sessions.
     */
    public void broadcast(String eventType, Object payload) {
        if (sessions.isEmpty()) {
            return;
        }

        try {
            String jsonMessage = mapper.writeValueAsString(Map.of(
                    "type", eventType,
                    "timestamp", System.currentTimeMillis(),
                    "data", payload
            ));

            TextMessage message = new TextMessage(jsonMessage);
            for (WebSocketSession session : sessions) {
                if (session.isOpen()) {
                    try {
                        synchronized (session) {
                            session.sendMessage(message);
                        }
                    } catch (IOException e) {
                        log.warn("Failed to deliver message to session {}", session.getId(), e);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to serialize broadcast payload", e);
        }
    }

    private void sendDirect(WebSocketSession session, String eventType, Object payload) {
        try {
            String jsonMessage = mapper.writeValueAsString(Map.of(
                    "type", eventType,
                    "timestamp", System.currentTimeMillis(),
                    "data", payload
            ));
            synchronized (session) {
                session.sendMessage(new TextMessage(jsonMessage));
            }
        } catch (IOException e) {
            log.warn("Failed to send welcome message to session {}", session.getId(), e);
        }
    }

    public int getActiveSessionCount() {
        return sessions.size();
    }
}
