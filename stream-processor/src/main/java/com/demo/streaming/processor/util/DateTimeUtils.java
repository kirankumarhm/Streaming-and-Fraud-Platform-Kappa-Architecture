package com.demo.streaming.processor.util;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

/**
 * Production date-time utility for stream window alignment and timestamp manipulation.
 */
public final class DateTimeUtils {

    private DateTimeUtils() {
        // Prevent instantiation
    }

    /**
     * Floors an instant to the start of a tumbling window of given duration in seconds.
     *
     * @param timestamp       Event instant.
     * @param durationSeconds Size of the tumbling window.
     * @return Floored window start instant.
     */
    public static Instant floorToWindowStart(Instant timestamp, long durationSeconds) {
        Objects.requireNonNull(timestamp, "timestamp must not be null");
        if (durationSeconds <= 0) {
            throw new IllegalArgumentException("durationSeconds must be strictly positive");
        }
        long epochSec = timestamp.getEpochSecond();
        long windowStartSec = (epochSec / durationSeconds) * durationSeconds;
        return Instant.ofEpochSecond(windowStartSec);
    }

    /**
     * Calculates the exclusive upper bound of a window given its start and size.
     */
    public static Instant calculateWindowEnd(Instant windowStart, long durationSeconds) {
        Objects.requireNonNull(windowStart, "windowStart must not be null");
        return windowStart.plus(durationSeconds, ChronoUnit.SECONDS);
    }
}

