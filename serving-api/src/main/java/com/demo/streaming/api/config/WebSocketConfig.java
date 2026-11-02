package com.demo.streaming.api.config;

import com.demo.streaming.api.websocket.StreamingEventWebSocketHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/**
 * WebSocket configuration registering real-time push event channels.
 */
@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final StreamingEventWebSocketHandler eventWebSocketHandler;

    public WebSocketConfig(StreamingEventWebSocketHandler eventWebSocketHandler) {
        this.eventWebSocketHandler = eventWebSocketHandler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(eventWebSocketHandler, "/ws/events")
                .setAllowedOrigins("*");
    }
}
