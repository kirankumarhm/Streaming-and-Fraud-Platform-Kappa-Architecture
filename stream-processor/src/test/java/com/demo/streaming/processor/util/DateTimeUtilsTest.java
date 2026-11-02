package com.demo.streaming.processor.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class DateTimeUtilsTest {

    @Test
    @DisplayName("Floor timestamp to 60-second tumbling window boundary")
    void floorToWindowStart() {
        // 2026-11-02T12:34:56Z
        Instant eventTime = Instant.parse("2026-11-02T12:34:56Z");
        Instant windowStart = DateTimeUtils.floorToWindowStart(eventTime, 60);

        // Should be floored to 2026-11-02T12:34:00Z
        assertThat(windowStart).isEqualTo(Instant.parse("2026-11-02T12:34:00Z"));

        Instant windowEnd = DateTimeUtils.calculateWindowEnd(windowStart, 60);
        assertThat(windowEnd).isEqualTo(Instant.parse("2026-11-02T12:35:00Z"));
    }
}

