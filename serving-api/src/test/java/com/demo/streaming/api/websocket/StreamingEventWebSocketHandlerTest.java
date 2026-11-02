package com.demo.streaming.api.websocket;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests verifying real-time WebSocket connection lifecycle and broadcast mechanics.
 */
@ExtendWith(MockitoExtension.class)
class StreamingEventWebSocketHandlerTest {

    @Test
    @DisplayName("afterConnectionEstablished adds session and delivers welcome handshake event")
    void afterConnectionEstablishedSuccess() throws IOException {
        StreamingEventWebSocketHandler handler = new StreamingEventWebSocketHandler();
        WebSocketSession session = mock(WebSocketSession.class);
        when(session.getId()).thenReturn("sess-1");

        handler.afterConnectionEstablished(session);

        assertThat(handler.getActiveSessionCount()).isEqualTo(1);
        ArgumentCaptor<TextMessage> captor = ArgumentCaptor.forClass(TextMessage.class);
        verify(session).sendMessage(captor.capture());
        assertThat(captor.getValue().getPayload()).contains("SYSTEM_CONNECTED").contains("sess-1");
    }

    @Test
    @DisplayName("afterConnectionClosed cleanly decrements active session count")
    void afterConnectionClosedSuccess() {
        StreamingEventWebSocketHandler handler = new StreamingEventWebSocketHandler();
        WebSocketSession session = mock(WebSocketSession.class);
        when(session.getId()).thenReturn("sess-2");

        handler.afterConnectionEstablished(session);
        assertThat(handler.getActiveSessionCount()).isEqualTo(1);

        handler.afterConnectionClosed(session, CloseStatus.NORMAL);
        assertThat(handler.getActiveSessionCount()).isZero();
    }

    @Test
    @DisplayName("broadcast pushes message payload to all open sessions")
    void broadcastDeliversToOpenSessions() throws IOException {
        StreamingEventWebSocketHandler handler = new StreamingEventWebSocketHandler();
        WebSocketSession session1 = mock(WebSocketSession.class);
        WebSocketSession session2 = mock(WebSocketSession.class);

        when(session1.getId()).thenReturn("s1");
        when(session2.getId()).thenReturn("s2");
        when(session1.isOpen()).thenReturn(true);
        when(session2.isOpen()).thenReturn(true);

        handler.afterConnectionEstablished(session1);
        handler.afterConnectionEstablished(session2);

        handler.broadcast("METRICS_UPDATE", Map.of("key", "value123"));

        ArgumentCaptor<TextMessage> captor = ArgumentCaptor.forClass(TextMessage.class);
        verify(session1, atLeastOnce()).sendMessage(captor.capture());
        verify(session2, atLeastOnce()).sendMessage(captor.capture());

        assertThat(captor.getValue().getPayload()).contains("METRICS_UPDATE").contains("value123");
    }

    @Test
    @DisplayName("broadcast gracefully handles closed sessions without failing other clients")
    void broadcastIgnoresClosedSessions() throws IOException {
        StreamingEventWebSocketHandler handler = new StreamingEventWebSocketHandler();
        WebSocketSession sessionOpen = mock(WebSocketSession.class);
        WebSocketSession sessionClosed = mock(WebSocketSession.class);

        when(sessionOpen.getId()).thenReturn("s-open");
        when(sessionClosed.getId()).thenReturn("s-closed");
        when(sessionOpen.isOpen()).thenReturn(true);
        when(sessionClosed.isOpen()).thenReturn(false);

        handler.afterConnectionEstablished(sessionOpen);
        handler.afterConnectionEstablished(sessionClosed);

        // Clear welcome message counts
        clearInvocations(sessionOpen, sessionClosed);

        handler.broadcast("TEST_EVENT", Map.of("foo", "bar"));

        verify(sessionOpen, times(1)).sendMessage(any(TextMessage.class));
        verify(sessionClosed, never()).sendMessage(any(TextMessage.class));
    }

    @Test
    @DisplayName("handleTransportError removes failing session")
    void handleTransportErrorRemovesSession() {
        StreamingEventWebSocketHandler handler = new StreamingEventWebSocketHandler();
        WebSocketSession session = mock(WebSocketSession.class);
        when(session.getId()).thenReturn("sess-err");

        handler.afterConnectionEstablished(session);
        assertThat(handler.getActiveSessionCount()).isEqualTo(1);

        handler.handleTransportError(session, new IOException("Connection reset by peer"));
        assertThat(handler.getActiveSessionCount()).isZero();
    }
}

