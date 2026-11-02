# Enterprise Production-Grade Streaming & Fraud Platform (Kappa Architecture)

An enterprise production-grade implementation of **Kappa Architecture** featuring **Redpanda**, stateful stream processing with design patterns (Strategy, Registry, Repository), and a **Spring Boot 3** REST API with **OpenAPI 3 / Swagger**, **RFC 7807 Problem Details**, and **Micrometer/Prometheus** telemetry.

---

## 1. End-to-End System & Data Flow Architecture

```mermaid
flowchart TD
    subgraph Producers["1. Ingestion Layer"]
        Gen["Transaction Generator\n(Normal + Anomaly Injections)"]
        ClientApps["Client Applications / Payment APIs"]
    end

    subgraph Broker["2. Streaming Broker (Redpanda Cluster)"]
        MainTopic["Topic: payment.transactions.v1\n(Ordered, Append-Only Commit Log)"]
        DLQTopic["Topic: payment.transactions.dlq\n(Poison Pill Isolation)"]
        Console["Redpanda Console UI\n(Port 8080 - Live Lag & Throughput)"]
    end

    subgraph Processor["3. Kappa Stream Processor (Java 21)"]
        Consumer["Kafka Consumer Loop\n(Manual Offset Commits)"]
        
        subgraph Pipeline["Processing Pipeline & Design Patterns"]
            Deser["JSON Deserializer & Boundary Validator"]
            WindowAgg["WindowAggregator\n(1-Minute Tumbling Buckets)"]
            
            subgraph Strategy["Fraud Rule Engine (Strategy Pattern)"]
                Registry["FraudRuleRegistry"]
                Rule1["LargeAmountSpikeRuleStrategy\n(Amount > $10,000)"]
                Rule2["HighVelocityBurstRuleStrategy\n(> 3 TX in 10s)"]
                Rule3["ImpossibleTravelRuleStrategy\n(GeoUtils Haversine > 1000km in 5m)"]
            end
        end

        DbSink["DbSink\n(HikariCP Pool & Idempotent Upserts)"]
        ReplayEngine["Offset Rewind Handler\n(--replay flag from Offset 0)"]
    end

    subgraph Storage["4. Serving Store (PostgreSQL 16)"]
        MetricsTable[("account_metrics_1min\n(Rollup Counts, Total Volume, Max Amount)")]
        AlertsTable[("fraud_alerts\n(Rule, Severity, Metadata, Resolution Status)")]
    end

    subgraph ServingAPI["5. Serving API (Spring Boot 3 - Port 8082)"]
        direction TB
        ControllerLayer["Controllers (OpenAPI 3 / Swagger Annotations)"]
        ServiceLayer["Service Layer (@Transactional)"]
        RepoLayer["Repository Layer (JdbcTemplate / DAOs)"]
        GlobalHandler["@RestControllerAdvice (RFC 7807 Problem Details)"]
        Actuator["Actuator Health & Prometheus (/actuator)"]
    end

    subgraph Clients["6. Consumers & Dashboards"]
        BrowserUI["Web Dashboards / Swagger UI\n(http://localhost:8082/swagger-ui.html)"]
        FraudOfficer["Fraud Analyst Triage"]
        PrometheusServer["Prometheus & Grafana"]
    end

    %% Data Flow Connections
    Gen -->|Idempotent Produce acks=all| MainTopic
    ClientApps -->|Produce Events| MainTopic
    MainTopic -.-> Console

    MainTopic -->|Poll Batches| Consumer
    Consumer --> Deser
    Deser -->|Corrupt Payload| DLQTopic
    Deser --> WindowAgg
    Deser --> Registry

    Registry --> Rule1
    Registry --> Rule2
    Registry --> Rule3

    WindowAgg -->|Snapshot| DbSink
    Rule1 & Rule2 & Rule3 -->|Detected Alert| DbSink
    ReplayEngine -.->|Reset Offset to 0| Consumer

    DbSink -->|INSERT ... ON CONFLICT DO UPDATE| MetricsTable
    DbSink -->|INSERT ... ON CONFLICT DO NOTHING| AlertsTable

    MetricsTable & AlertsTable --> RepoLayer
    RepoLayer --> ServiceLayer
    ServiceLayer --> ControllerLayer
    ControllerLayer -.-> GlobalHandler

    ControllerLayer --> BrowserUI
    ControllerLayer --> FraudOfficer
    Actuator --> PrometheusServer
```

---

## 2. Strategy Pattern & Clean Architecture Class Hierarchy

```mermaid
classDiagram
    direction TB

    %% Strategy Pattern for Fraud Detection
    class FraudRuleStrategy {
        <<interface>>
        +getRuleName() String
        +getDefaultSeverity() Severity
        +evaluate(current, history) Optional~FraudAlert~
    }

    class LargeAmountSpikeRuleStrategy {
        -BigDecimal threshold
        +evaluate(current, history) Optional~FraudAlert~
    }

    class HighVelocityBurstRuleStrategy {
        -int burstCountThreshold
        -Duration windowDuration
        +evaluate(current, history) Optional~FraudAlert~
    }

    class ImpossibleTravelRuleStrategy {
        -double maxDistanceKm
        -Duration maxTimeDifference
        +evaluate(current, history) Optional~FraudAlert~
    }

    FraudRuleStrategy <|.. LargeAmountSpikeRuleStrategy : implements
    FraudRuleStrategy <|.. HighVelocityBurstRuleStrategy : implements
    FraudRuleStrategy <|.. ImpossibleTravelRuleStrategy : implements

    class FraudRuleRegistry {
        -List~FraudRuleStrategy~ strategies
        -Map~String, Deque~PaymentTransaction~~ accountState
        +register(strategy)
        +evaluateAll(current) List~FraudAlert~
        +cleanupInactiveAccounts()
    }

    FraudRuleRegistry o-- FraudRuleStrategy : executes

    %% Clean Layered Architecture in Spring Boot
    class AccountMetricsController {
        -AccountMetricsService service
        +getAccountMetrics(accountId, limit) ResponseEntity
    }

    class AccountMetricsService {
        <<interface>>
        +getRecentAccountMetrics(accountId, limit) List~AccountMetricsResponse~
    }

    class AccountMetricsServiceImpl {
        -AccountMetricsRepository repository
        +getRecentAccountMetrics(accountId, limit) List~AccountMetricsResponse~
    }

    class AccountMetricsRepository {
        <<interface>>
        +findLatestMetricsByAccountId(accountId, limit) List~AccountMetricsResponse~
    }

    class AccountMetricsRepositoryImpl {
        -JdbcTemplate jdbcTemplate
        +findLatestMetricsByAccountId(accountId, limit) List~AccountMetricsResponse~
    }

    AccountMetricsController --> AccountMetricsService : delegates
    AccountMetricsService <|.. AccountMetricsServiceImpl : implements
    AccountMetricsServiceImpl --> AccountMetricsRepository : queries
    AccountMetricsRepository <|.. AccountMetricsRepositoryImpl : implements
```

---

## 3. Kappa Historical Reprocessing Sequence Diagram (`--replay`)

```mermaid
sequenceDiagram
    autonumber
    actor Operator as Data Engineer
    participant App as Kappa Stream Processor
    participant Redpanda as Redpanda Broker (Commit Log)
    participant Engine as WindowAggregator & FraudRegistry
    participant DB as PostgreSQL Serving Store

    Note over Operator,App: Scenario: Business deploys an updated fraud rule
    Operator->>App: Stop processor & restart with --replay
    App->>App: Generate ephemeral group ID (kappa-replay-processor-<timestamp>)
    App->>Redpanda: Connect & subscribe (auto.offset.reset = earliest)
    Redpanda-->>App: Stream all historical events from Offset 0
    
    loop Stream All Historical Records
        App->>Engine: Re-evaluate transactions against new rules
        Engine->>DB: Idempotent upsert into account_metrics_1min
        Engine->>DB: Idempotent insert into fraud_alerts
    end

    App->>App: Historical log caught up to head
    Note over App,DB: Serving store is 100% updated without any separate batch cluster!
```

---

## 4. Enterprise Standards & Design Patterns Implemented

| Category | Implementation Details |
| :--- | :--- |
| **Strategy Pattern** | Pluggable `FraudRuleStrategy` interface with modular rules (`LargeAmountSpikeRuleStrategy`, `HighVelocityBurstRuleStrategy`, `ImpossibleTravelRuleStrategy`). |
| **Clean Architecture** | Strict separation: Controller ➔ Service (Interface + Impl) ➔ Repository (Interface + Impl) ➔ DTOs / Entities. |
| **Exception Handling** | Centralized `@RestControllerAdvice` mapping exceptions to standard **RFC 7807 / RFC 9457 Problem Details** with field-level validation errors. |
| **API Documentation** | Automated **OpenAPI 3 (Swagger UI)** documentation available at `/swagger-ui.html`. |
| **Validation** | Jakarta Bean Validation (`@NotNull`, `@NotBlank`, `@Size`, `@DecimalMin`, `@Valid`). |
| **Observability** | Custom Spring Actuator `HealthIndicator` (`/actuator/health`) and Prometheus scraping metrics (`/actuator/prometheus`). |
| **Stream Resilience** | Manual offset commits, tuned HikariCP connection pool, and automated routing of poison pills to a **Dead Letter Queue (DLQ)**. |
| **Log Traceability** | Structured logging with SLF4J MDC attaching `accountId` and `txId` correlation tags. |

---

## 5. Quick Start

### Step 1: Start Infrastructure (Redpanda + Postgres + Console)
```bash
./streaming-platform/scripts/run-infrastructure.sh
```

* **Redpanda Broker:** `localhost:9092`
* **Redpanda Console UI:** [http://localhost:8080](http://localhost:8080)
* **PostgreSQL DB:** `localhost:5432` (`streaming_db` / user: `postgres` / pass: `postgrespassword`)

---

### Step 2: Build the Applications
Using Maven (Java 21):
```bash
cd streaming-platform
mvn clean package -DskipTests
```

---

### Step 3: Run the Services (3 Terminal Windows)

#### Terminal 1: Run the Kappa Stream Processor
```bash
java -jar stream-processor/target/stream-processor-1.0.0-SNAPSHOT.jar
```
*Listens to `payment.transactions.v1`, runs stateful window aggregations, evaluates fraud strategy rules, and flushes to PostgreSQL.*

#### Terminal 2: Run the Transaction Generator
```bash
java -jar transaction-generator/target/transaction-generator-1.0.0-SNAPSHOT.jar
```
*Emits transactions with idempotent delivery and periodically injects anomalies.*

#### Terminal 3: Run the Spring Boot 3 Serving API
```bash
java -jar serving-api/target/serving-api-1.0.0-SNAPSHOT.jar
```
*Launches the REST API on port `8082`.*

---

## 6. API Documentation & Endpoints

### Swagger UI (Interactive API Explorer)
Open [http://localhost:8082/swagger-ui.html](http://localhost:8082/swagger-ui.html) to view all endpoints, request/response models, and test them live.

### High-Level Summary
```bash
curl http://localhost:8082/api/v1/stats
```

### Account Metrics (1-Minute Tumbling Windows)
```bash
curl http://localhost:8082/api/v1/accounts/ACC-1002/metrics
```

### Query Fraud Alerts
```bash
curl "http://localhost:8082/api/v1/fraud/alerts?severity=CRITICAL"
```

### Resolve a Fraud Alert (Validation Protected)
```bash
curl -X POST http://localhost:8082/api/v1/fraud/alerts/<ALERT_ID>/resolve \
     -H "Content-Type: application/json" \
     -d '{"note": "Cardholder confirmed transaction over the phone.", "resolvedBy": "fraud_officer_101"}'
```

### Health & Metrics Telemetry
* **Actuator Health:** `curl http://localhost:8082/actuator/health`
* **Prometheus Metrics:** `curl http://localhost:8082/actuator/prometheus`

---

## 7. Kappa Historical Reprocessing Demonstration

To demonstrate Kappa's core advantage—recomputing state without a batch cluster:
1. Stop the stream processor.
2. Restart it with the `--replay` flag:
   ```bash
   java -jar stream-processor/target/stream-processor-1.0.0-SNAPSHOT.jar --replay
   ```
3. The processor creates an ephemeral consumer group, connects to Redpanda, rewinds to offset `0`, and recalculates all historical metrics and fraud states from scratch using the **exact same codebase**!
