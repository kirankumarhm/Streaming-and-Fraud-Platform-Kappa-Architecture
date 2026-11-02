package com.demo.streaming.processor.exception;

/**
 * Root unchecked exception for all stream processing failures.
 */
public class StreamProcessingException extends RuntimeException {
    public StreamProcessingException(String message) {
        super(message);
    }

    public StreamProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}

