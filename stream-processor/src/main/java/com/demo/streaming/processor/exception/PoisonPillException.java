package com.demo.streaming.processor.exception;

/**
 * Thrown when an unparseable or schema-violating payload is encountered in a topic.
 */
public class PoisonPillException extends StreamProcessingException {
    private final String rawPayload;
    private final long offset;

    public PoisonPillException(String rawPayload, long offset, String reason, Throwable cause) {
        super(String.format("Poison pill encountered at offset %d: %s", offset, reason), cause);
        this.rawPayload = rawPayload;
        this.offset = offset;
    }

    public String getRawPayload() {
        return rawPayload;
    }

    public long getOffset() {
        return offset;
    }
}

