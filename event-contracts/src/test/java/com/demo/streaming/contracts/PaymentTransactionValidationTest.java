package com.demo.streaming.contracts;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PaymentTransactionValidationTest {

    private static Validator validator;

    @BeforeAll
    static void setUp() {
        ValidatorFactory factory = Validation.byDefaultProvider()
                .configure()
                .messageInterpolator(new ParameterMessageInterpolator())
                .buildValidatorFactory();
        validator = factory.getValidator();
    }

    @Test
    @DisplayName("Valid payment transaction passes bean validation and defensive checks")
    void validTransactionPasses() {
        PaymentTransaction tx = new PaymentTransaction(
                "tx-12345",
                "ACC-1001",
                "AMZN-US",
                BigDecimal.valueOf(150.75),
                "USD",
                new PaymentTransaction.Location("New York", "USA", 40.7128, -74.0060),
                "FP-CHROME-99",
                Instant.now()
        );

        Set<ConstraintViolation<PaymentTransaction>> violations = validator.validate(tx);
        assertThat(violations).isEmpty();
        assertThat(tx.currency()).isEqualTo("USD");
    }

    @Test
    @DisplayName("Invalid currency pattern fails validation")
    void invalidCurrencyFails() {
        PaymentTransaction tx = new PaymentTransaction(
                "tx-12345",
                "ACC-1001",
                "AMZN-US",
                BigDecimal.valueOf(150.75),
                "US_DOLLARS", // Invalid pattern (must be 3 uppercase letters)
                null,
                "FP-CHROME-99",
                Instant.now()
        );

        Set<ConstraintViolation<PaymentTransaction>> violations = validator.validate(tx);
        assertThat(violations).hasSize(1);
        assertThat(violations.iterator().next().getMessage()).contains("Currency must be a valid 3-letter ISO code");
    }

    @Test
    @DisplayName("Invalid latitude throws IllegalArgumentException defensively")
    void invalidLocationThrows() {
        assertThatThrownBy(() -> new PaymentTransaction.Location("Tokyo", "Japan", 95.0, 139.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Latitude must be between -90 and 90");
    }
}
