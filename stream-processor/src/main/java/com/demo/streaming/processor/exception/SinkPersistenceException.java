package com.demo.streaming.processor.exception;

/**
 * Thrown when persistence to the serving store fails after exhausted retries.
 */
public class SinkPersistenceException extends StreamProcessingException {
    public SinkPersistenceException(String message, Throwable cause) {
        super(message, cause);
    }
}

