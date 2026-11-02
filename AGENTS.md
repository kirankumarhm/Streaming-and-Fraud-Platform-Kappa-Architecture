# Workspace Agent Guidelines & Production Standards

All agentic AI coding activities within this workspace MUST strictly follow the enterprise production standards defined in:
👉 [.agents/rules/production-grade-standards.md](file:///.agents/rules/production-grade-standards.md)

### Key Enforcements:
1. **Clean Architecture:** Strict separation between Controller, Service (Interface + Implementation), Repository/DAO, and Domain layers.
2. **Spring Boot Standards:** Centralized `@RestControllerAdvice` exception handling with RFC 7807/9457 Problem Details, Jakarta Validation on all endpoints/DTOs, and type-safe `@ConfigurationProperties`.
3. **API Documentation:** Comprehensive OpenAPI 3 / Swagger annotations (`@Tag`, `@Operation`, `@ApiResponse`, `@Schema`) on all public endpoints and models.
4. **Design Patterns:** Proper use of Strategy, Factory, Builder, Repository, and Template patterns.
5. **Observability & Resilience:** Spring Boot Actuator health checks, custom metrics via Micrometer/Prometheus, connection pool tuning, and structured logging with MDC.
6. **Documentation & Utilities:** Full Javadoc on interfaces, stateless utility classes, and zero magic numbers.

