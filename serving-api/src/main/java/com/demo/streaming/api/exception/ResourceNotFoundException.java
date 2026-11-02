package com.demo.streaming.api.exception;

/**
 * Thrown when an entity requested by ID does not exist in the serving database.
 */
public class ResourceNotFoundException extends RuntimeException {
    public ResourceNotFoundException(String message) {
        super(message);
    }
}

