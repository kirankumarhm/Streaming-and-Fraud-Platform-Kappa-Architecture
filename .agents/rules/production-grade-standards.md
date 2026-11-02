# Antigravity Enterprise Production-Grade Engineering Standards

> **Rule Scope:** Global / Enterprise Production Standard
> **Applies to:** All backend engineering, microservices, data streaming pipelines, APIs, and cloud services authored or refactored by the agent.

---

## 1. Core Architectural & Design Pattern Principles

1. **Layered & Clean Architecture (Separation of Concerns):**
   * **Domain Layer:** Pure business entities, value objects, and domain exceptions. Zero framework dependencies.
   * **Service / Application Layer:** Business logic orchestration, interfaces (`FooService`) and concrete implementations (`FooServiceImpl`).
   * **Adapter / Repository Layer:** Encapsulates all persistence and I/O. Never leak SQL, ORM entities, or raw database types into the controller layer.
   * **Web / Controller Layer:** Pure routing, deserialization, HTTP status mapping, and OpenAPI contracts. Zero business logic in controllers.

2. **Design Pattern Mandates:**
   * **Strategy Pattern:** Use for dynamic rule evaluation, algorithms, payment gateways, and custom processing strategies (e.g., `FraudRuleStrategy`).
   * **Factory / Builder Pattern:** Use for constructing complex domain entities and DTOs with validation at instantiation boundaries.
   * **Chain of Responsibility:** Use for sequential filtering, validation pipelines, and interceptors.
   * **Template Method:** Use for base processors, stream consumers, and retry handlers.
   * **Repository / DAO Pattern:** Abstract all database interactions behind type-safe interfaces.

3. **Immutability & Value Objects:**
   * Prefer modern Java `record` types for DTOs, events, and value objects.
   * All fields must be non-nullable by default unless explicitly documented and checked.

---

## 2. Spring Boot Production Standards

1. **Input Validation (Jakarta Bean Validation):**
   * Every request payload must be annotated with `@Valid` on the controller method parameter.
   * DTO fields must include explicit constraints (`@NotNull`, `@NotBlank`, `@Size`, `@Pattern`, `@Positive`, `@Email`).
   * Group validation constraints when create and update semantics differ.

2. **Centralized Exception Handling (`@RestControllerAdvice`):**
   * Never let unhandled exceptions leak stack traces or internal implementation details to the caller.
   * Implement a centralized `@RestControllerAdvice` defining unified JSON error responses conforming to **RFC 7807 / RFC 9457 (Problem Details)**:
     ```json
     {
       "timestamp": "2026-11-02T21:00:00Z",
       "status": 400,
       "error": "Bad Request",
       "code": "VALIDATION_FAILED",
       "message": "Input validation failed on 2 fields",
       "path": "/api/v1/accounts/123/metrics",
       "correlationId": "trace-uuid-123",
       "fieldErrors": [
         { "field": "amount", "rejectedValue": -5.00, "message": "Amount must be positive" }
       ]
     }
     ```
   * Maintain a strong domain exception hierarchy (`ResourceNotFoundException`, `BusinessRuleViolationException`, `ConflictException`, `IntegrationException`).

3. **Configuration & Environment Management:**
   * **No hardcoded credentials, URLs, or ports.**
   * Use type-safe `@ConfigurationProperties` annotated with `@Validated` instead of scattered `@Value` annotations.
   * Supply sensible defaults and document environment variable overrides in `application.yml`.

4. **API Documentation (OpenAPI 3 / Swagger):**
   * Every controller must have `@Tag` with human-readable descriptions.
   * Every endpoint must have `@Operation(summary = "...", description = "...")` and explicit `@ApiResponses` documenting `200`, `400`, `404`, and `500` status codes.
   * DTO fields must include `@Schema` annotations with descriptions and realistic example values.

5. **Observability, Health & Telemetry:**
   * Expose Spring Boot Actuator endpoints (`health`, `info`, `metrics`, `prometheus`).
   * Implement custom `HealthIndicator` beans for external dependencies (brokers, databases, remote caches).
   * Record custom business metrics using `MeterRegistry` (Counters, Timers, Gauges).
   * Use MDC (Mapped Diagnostic Context) to attach `correlationId`, `accountId`, and `transactionId` to all structured log lines.

---

## 3. Stream Processing & Messaging Standards (Kafka / Redpanda / Flink)

1. **Producer Standards:**
   * Must enable idempotence (`enable.idempotence=true`).
   * Must set `acks=all` for financial/critical transactional topics.
   * Configure realistic retries, backoff, and compression (`snappy` or `zstd`).

2. **Consumer Standards:**
   * Use **manual offset commit** after successful persistence or processing (at-least-once or exactly-once semantics).
   * Never silently swallow processing exceptions inside consumer loops.
   * **Dead Letter Queue (DLQ):** Route poison pills or unparseable payloads to a `.dlq` topic with original payload and exception metadata in headers.
   * Provide a clean graceful shutdown hook via `consumer.wakeup()` and shutdown listeners.

3. **Database Sink Standards:**
   * Always use a production connection pool (HikariCP) with tuned connection timeouts, max lifetime, and leak detection thresholds.
   * Batch upserts using `INSERT ... ON CONFLICT DO UPDATE` or transactional two-phase commit to ensure idempotency.

---

## 4. Code Quality, Documentation & Utilities

1. **Documentation Integrity:**
   * All public interfaces, service methods, and domain entities must include clear Javadoc documenting contracts, parameters, return types, and thrown exceptions.
2. **Utilities:**
   * Isolate common calculation logic (geo-spatial, date-time manipulation, financial currency rounding) into static or stateless component utility classes with private constructors (`@UtilityClass` pattern).
3. **No Magic Numbers or Strings:**
   * Define static final constants with descriptive naming or dedicated enum types.

