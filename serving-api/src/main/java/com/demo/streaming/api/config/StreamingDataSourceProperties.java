package com.demo.streaming.api.config;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Type-safe, validated datasource configuration properties.
 */
@Validated
@ConfigurationProperties(prefix = "streaming.datasource")
public record StreamingDataSourceProperties(
        @NotBlank(message = "JDBC URL is mandatory") String url,
        @NotBlank(message = "DB username is mandatory") String username,
        @NotBlank(message = "DB password is mandatory") String password,
        String driverClassName
) {}

