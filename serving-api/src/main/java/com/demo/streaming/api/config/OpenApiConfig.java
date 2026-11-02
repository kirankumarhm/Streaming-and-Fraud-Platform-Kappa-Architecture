package com.demo.streaming.api.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * OpenAPI 3 / Swagger configuration providing automated API contract documentation.
 */
@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI streamingPlatformOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Real-Time Streaming Analytics & Fraud Serving API")
                        .description("Production-grade RESTful API providing real-time sliding window aggregates, fraud alert triage, and system health metrics.")
                        .version("1.0.0")
                        .contact(new Contact()
                                .name("Data Engineering Team")
                                .email("data-engineering@demo.com"))
                        .license(new License()
                                .name("Apache 2.0")
                                .url("https://www.apache.org/licenses/LICENSE-2.0")))
                .servers(List.of(
                        new Server().url("http://localhost:8082").description("Local Development Server")
                ));
    }
}

