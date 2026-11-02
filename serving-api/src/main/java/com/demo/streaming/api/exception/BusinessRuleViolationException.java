package com.demo.streaming.api.exception;

/**
 * Thrown when an operation violates domain state invariants (e.g. resolving an already-resolved alert).
 */
public class BusinessRuleViolationException extends RuntimeException {
    public BusinessRuleViolationException(String message) {
        super(message);
    }
}

