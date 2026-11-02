# Real-Time Data Architectures: Kappa vs. Lambda in Enterprise Production

[![Java 21](https://img.shields.io/badge/Java-21-orange.svg)](https://openjdk.org/projects/jdk/21/)
[![Spring Boot 3.3.4](https://img.shields.io/badge/Spring%20Boot-3.3.4-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![Redpanda v24](https://img.shields.io/badge/Redpanda-v24.2.14-red.svg)](https://redpanda.com/)
[![PostgreSQL 16](https://img.shields.io/badge/PostgreSQL-16-blue.svg)](https://www.postgresql.org/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

An enterprise production-grade implementation and side-by-side benchmark of **Kappa Architecture** ($\kappa$) and **Lambda Architecture** ($\Lambda$), inspired by Natesh Somanna's foundational analysis on distributed big data processing systems:
👉 **[Read the Reference Article on Medium: Kappa vs. Lambda Architecture in Large-scale Data Processing Systems](https://medium.com/@natesh.somanna/kappa-vs-lambda-architecture-in-large-scale-data-processing-systems-1da30973591d)**

---

## 1. Conceptual Foundation: What the Article is Talking About

In modern enterprise data platforms, organizations face a fundamental engineering challenge:  
> *"How do we process massive historical datasets with 100% precision and fault tolerance while simultaneously analyzing incoming live events with sub-second latency?"*

Historically, relational databases and standard OLAP warehouses could not handle this dual constraint at scale. Two primary distributed system paradigms emerged:

---

### Lambda Architecture ($\Lambda$)
* **Origin:** Proposed by **Nathan Marz** (creator of Apache Storm and author of *Big Data*) in 2011.
* **Core Philosophy:** **"Batch for accuracy, streaming for low latency."**
* **Structure:** The system is split into three distinct layers:
  1. **Batch Layer:** Periodically recomputes the entire master dataset (historically via Hadoop MapReduce, Apache Spark, or Hive) to generate authoritative, immutable batch views. Highly accurate, but high latency (hours to days).
  2. **Speed Layer:** Processes incoming data in real time (via Apache Storm, Flink, or streaming engines) to fill the gap for recent events not yet processed by the batch layer. Low latency (milliseconds), but may tolerate eventual consistency or rough approximations.
  3. **Serving Layer:** Indexes both the batch views and the real-time speed views to answer queries via the classic Lambda formula:
     $$\text{Query} = \text{Merge}(\text{Batch View}, \text{Real-time Speed View})$$
* **The Fatal Flaw of Lambda:** **Dual-Codebase Synchronization Overhead.**  
  Engineers must write and maintain their business logic **twice**—once in the batch codebase (e.g., Spark SQL / MapReduce) and once in the streaming codebase (e.g., Storm / Flink). Keeping algorithms, edge cases, and bug fixes synchronized across two disparate systems is notoriously difficult and error-prone.

---

### Kappa Architecture ($\kappa$)
* **Origin:** Proposed by **Jay Kreps** (creator of Apache Kafka and CEO of Confluent) in 2014 as an evolution and simplification.
* **Core Philosophy:** **"Everything is an infinite event stream."**
* **Structure:** Eliminates the batch layer entirely.
  1. **Immutable Log:** All data flows into an ordered, append-only distributed commit log (Apache Kafka, Redpanda) that acts as the single source of truth.
  2. **Unified Stream Processor:** A single stateful stream processing engine (Apache Flink, Kafka Streams, or Reactive Java) handles all data processing using **one unified codebase**.
  3. **Historical Reprocessing via Offset Rewind:** If business rules change or bugs are fixed, you do **not** run a separate batch pipeline. Instead, you spin up a new instance of your stream processor, rewind its consumer group offset to `0` (the beginning of the topic), and re-stream the historical log through the updated logic into a new serving view.

---

### Side-by-Side Architectural Comparison

| Dimension | **Lambda Architecture ($\Lambda$)** | **Kappa Architecture ($\kappa$)** |
| :--- | :--- | :--- |
| **Philosophical Premise** | "Batch for accuracy, streaming for speed" | "Everything is an infinite event stream" |
| **Pipeline Count** | **2 Parallel Pipelines** (Batch + Speed) | **1 Single Unified Pipeline** (Stream Only) |
| **Codebases** | Dual (must maintain batch and stream logic) | Single (one unified streaming codebase) |
| **Serving Query** | $\text{Merge}(\text{Batch View}, \text{Speed View})$ | Direct query against stream-updated views |
| **Historical Reprocessing** | Execute batch jobs over archived Parquet/S3 files | Rewind consumer offset to `0` (`--replay`) |
| **Synchronization Risk** | High (divergence between batch & stream logic) | Zero (single source of business logic) |
| **Operational Stack** | Heavy (Kafka + Spark/Hadoop + Flink + DB) | Streamlined (Redpanda + Stream Engine + DB) |

---

## 2. System Architecture & End-to-End Data Flow

This repository implements **both architectures side-by-side** over the same real-time financial transaction event stream so you can directly inspect, compare, and verify their operational behaviors.

```mermaid
flowchart TD
    subgraph Producers["1. Ingestion Layer"]
        Gen["Transaction Generator\n(Normal Transactions + Injected Anomalies)"]
        ClientApps["Client Applications & Payment Gateways"]
    end

    subgraph Broker["2. Streaming Broker (Redpanda Cluster)"]
        MainTopic["Topic: payment.transactions.v1\n(Ordered, Immutable Commit Log)"]
        DLQTopic["Topic: payment.transactions.dlq\n(Poison Pill Isolation)"]
        Console["Redpanda Console UI\n(Port 8080 - Live Lag & Partitions)"]
    end

    subgraph KappaTrack["3. Kappa Architecture Track (Single Stream Engine)"]
        Consumer["Kappa Stream Processor\n(Manual Commit Sync)"]
        WindowAgg["WindowAggregator\n(1-Minute Tumbling Buckets)"]
        
        subgraph StrategyPattern["Fraud Rule Engine (Strategy Pattern)"]
            Registry["FraudRuleRegistry"]
            Rule1["LargeAmountSpikeRuleStrategy\n(Amount > $10,000)"]
            Rule2["HighVelocityBurstRuleStrategy\n(> 3 TX in 10s)"]
            Rule3["ImpossibleTravelRuleStrategy\n(Haversine > 1000km in 5m)"]
        end

        ReplayEngine["Offset Rewind Handler\n(--replay flag rewinds to Offset 0)"]
    end

    subgraph LambdaTrack["4. Lambda Architecture Track (Batch Layer Engine)"]
        BatchWorker["Lambda Batch Processor\n(Scheduled Periodic / One-Off Job)"]
        BatchAgg["BatchAggregator\n(Authoritative Daily Master Reconciler)"]
    end

    subgraph ServingStore["5. Serving Store (PostgreSQL 16)"]
        MetricsSpeed[("account_metrics_1min\n(Speed Layer / Kappa Real-Time Rollups)")]
        MetricsBatch[("account_metrics_batch\n(Lambda Batch Layer Reconciled Days)")]
        AlertsTable[("fraud_alerts\n(Rule, Severity, Forensic Metadata)")]
    end

    subgraph ServingAPI["6. Serving API & Dashboards (Spring Boot 3 - Port 8082)"]
        direction TB
        KappaAPI["GET /api/v1/accounts/{id}/metrics\n(Direct Stream Serving View)"]
        LambdaAPI["GET /api/v1/lambda/accounts/{id}/merged-view\n(Query = Merge(Batch, Speed))"]
        ComparisonAPI["GET /api/v1/comparison\n(Live Architectural Matrix)"]
        WebDashboard["Interactive Operations Dashboard\n(http://localhost:8082/dashboard.html)"]
        SwaggerUI["OpenAPI 3 / Swagger Explorer\n(http://localhost:8082/swagger-ui.html)"]
        Actuator["Prometheus & Health Telemetry\n(/actuator/prometheus)"]
    end

    %% Ingestion Links
    Gen -->|Idempotent Produce acks=all| MainTopic
    ClientApps -->|Publish Payment Events| MainTopic
    MainTopic -.-> Console

    %% Kappa Track Flow
    MainTopic -->|Poll Batches| Consumer
    Consumer --> WindowAgg
    Consumer --> Registry
    Consumer -.->|Corrupt Payloads| DLQTopic
    Registry --> Rule1 & Rule2 & Rule3
    WindowAgg -->|Idempotent Upsert| MetricsSpeed
    Rule1 & Rule2 & Rule3 -->|Persist Alert| AlertsTable
    ReplayEngine -.->|Rewind Offset to 0| Consumer

    %% Lambda Track Flow
    MainTopic -->|Read Master Log from 0| BatchWorker
    BatchWorker --> BatchAgg
    BatchAgg -->|Flush Reconciled Days| MetricsBatch

    %% Serving Layer Connections
    MetricsSpeed --> KappaAPI
    MetricsSpeed & MetricsBatch --> LambdaAPI
    MetricsSpeed & AlertsTable --> WebDashboard
```

---

## 3. Design Patterns & Clean Architecture Class Structure

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

## 4. Sequence Diagrams: Replay vs. Batch Merge

### 4.1. The Kappa Replay Sequence (`--replay`)
Demonstrating how historical data is reprocessed in Kappa without any batch infrastructure:

```mermaid
sequenceDiagram
    autonumber
    actor Operator as Data Engineer
    participant App as Kappa Stream Processor
    participant Redpanda as Redpanda Broker (Commit Log)
    participant Engine as WindowAggregator & FraudRegistry
    participant DB as PostgreSQL Serving Store

    Note over Operator,App: Scenario: Business updates fraud detection rules
    Operator->>App: Stop processor & launch with --replay flag
    App->>App: Create ephemeral group ID (kappa-replay-processor-<timestamp>)
    App->>Redpanda: Subscribe to topic (auto.offset.reset = earliest)
    Redpanda-->>App: Replay entire master event log from Offset 0
    
    loop Stream All Historical Records
        App->>Engine: Re-evaluate transactions through updated rule strategies
        Engine->>DB: Idempotent upsert into account_metrics_1min
        Engine->>DB: Idempotent insert into fraud_alerts
    end

    App->>App: Catch up to live topic head and resume real-time processing
    Note over App,DB: Serving store is 100% updated using the exact same codebase!
```

### 4.2. The Lambda Batch Merge Sequence (`Query = Merge(Batch, Speed)`)
Demonstrating how queries are answered in Lambda architecture:

```mermaid
sequenceDiagram
    autonumber
    actor Client as Dashboard / REST Client
    participant API as LambdaMergedViewController
    participant BatchDB as PostgreSQL (account_metrics_batch)
    participant SpeedDB as PostgreSQL (account_metrics_1min)

    Client->>API: GET /api/v1/lambda/accounts/ACC-1002/merged-view
    API->>BatchDB: Query authoritative historical reconciled days
    BatchDB-->>API: Return batch records (e.g. Nov 1, Nov 2)
    API->>SpeedDB: Query real-time tumbling windows (last 10 mins)
    SpeedDB-->>API: Return live streaming records
    API->>API: Apply Lambda Merge: Union volume and transaction counts
    API-->>Client: Return complete merged payload
```

---

## 5. Modules in this Project

```text
streaming-platform/
├── docker/
│   ├── docker-compose.yml          # Redpanda, Console, PostgreSQL, Prometheus, Grafana
│   ├── init-db.sql                 # Serving schemas for both Kappa & Lambda
│   └── prometheus/prometheus.yml   # Prometheus scraper configuration
├── event-contracts/                # Domain models with Jakarta Bean Validation
├── transaction-generator/          # High-velocity producer with injected fraud anomalies
├── stream-processor/               # KAPPA TRACK: Stateful stream engine + Strategy Pattern
├── lambda-batch-processor/         # LAMBDA TRACK: Batch layer master reconciler
├── serving-api/                    # Spring Boot 3 Serving REST API & Operations Dashboard
└── scripts/
    └── run-infrastructure.sh       # One-click startup for container infrastructure
```

---

## 6. Quick Start & Execution Guide

### Prerequisites
* Java 21+ installed
* Docker & Docker Compose (or Colima) installed
* Maven 3.9+ (or use IntelliJ's bundled Maven)

---

### Step 1: Start Container Infrastructure
```bash
./streaming-platform/scripts/run-infrastructure.sh
```
* **Redpanda Broker:** `localhost:9092`
* **Redpanda Console:** [http://localhost:8080](http://localhost:8080)
* **PostgreSQL Serving DB:** `localhost:5432` (`streaming_db` / user: `postgres` / pass: `postgrespassword`)
* **Prometheus:** `http://localhost:9090`
* **Grafana:** `http://localhost:3000` *(admin / admin)*

---

### Step 2: Build the Complete Platform
```bash
cd streaming-platform
mvn clean package -DskipTests
```

---

### Step 3: Run the Services

#### Terminal 1: Launch the Kappa Stream Processor
```bash
java -jar stream-processor/target/stream-processor-1.0.0-SNAPSHOT.jar
```

#### Terminal 2: Run the Transaction Generator
```bash
java -jar transaction-generator/target/transaction-generator-1.0.0-SNAPSHOT.jar
```

#### Terminal 3: Run the Lambda Batch Processor (Batch Layer)
To run a one-off batch reconciliation cycle:
```bash
java -jar lambda-batch-processor/target/lambda-batch-processor-1.0.0-SNAPSHOT.jar --run-once
```
*(Or run without `--run-once` to keep it running as a periodic daemon).*

#### Terminal 4: Launch the Spring Boot 3 Serving API & Dashboard
```bash
java -jar serving-api/target/serving-api-1.0.0-SNAPSHOT.jar
```

---

## 7. Interactive Dashboards & API Endpoints

### 7.1. Operations & Fraud Web Dashboard
Open your browser to:  
👉 **[http://localhost:8082](http://localhost:8082)** *(or [http://localhost:8082/dashboard.html](http://localhost:8082/dashboard.html))*

Features:
* **Tab 1: Kappa Streaming View:** Live real-time tumbling window metrics and fraud triage feed with 1-click incident resolution.
* **Tab 2: Lambda Dual-Layer Merged View:** Live side-by-side view showing the Batch Layer table (`account_metrics_batch`) and Speed Layer table (`account_metrics_1min`) merged into a single consolidated result.
* **Tab 3: Architectural Decision Matrix:** Interactive comparison of Lambda vs. Kappa trade-offs.

### 7.2. OpenAPI 3 / Swagger Explorer
👉 **[http://localhost:8082/swagger-ui.html](http://localhost:8082/swagger-ui.html)**

### 7.3. Key API Endpoints
* **Platform KPI Summary:**
  ```bash
  curl http://localhost:8082/api/v1/stats
  ```
* **Kappa Stream Metrics (1-Min Windows):**
  ```bash
  curl http://localhost:8082/api/v1/accounts/ACC-1002/metrics
  ```
* **Lambda Merged View ($\text{Query} = \text{Merge}(\text{Batch}, \text{Speed})$):**
  ```bash
  curl http://localhost:8082/api/v1/lambda/accounts/ACC-1002/merged-view
  ```
* **Architecture Comparison Matrix:**
  ```bash
  curl http://localhost:8082/api/v1/comparison
  ```
* **Query Critical Fraud Alerts:**
  ```bash
  curl "http://localhost:8082/api/v1/fraud/alerts?severity=CRITICAL"
  ```
* **Resolve a Fraud Incident (Validation Protected):**
  ```bash
  curl -X POST http://localhost:8082/api/v1/fraud/alerts/<ALERT_ID>/resolve \
       -H "Content-Type: application/json" \
       -d '{"note": "Cardholder verified overseas travel transaction.", "resolvedBy": "fraud_officer_42"}'
  ```

---

## 8. Automated Testing & Verification Suite

To run all automated unit and integration tests across the 5 modules:
```bash
cd streaming-platform
mvn test
```

### Verified Test Suites:
* **`FraudRuleStrategyTest`:** Unit tests verifying individual Strategy pattern implementations (`LargeAmountSpike`, `HighVelocityBurst`, `ImpossibleTravel`).
* **`WindowAggregatorTest`:** Verifies tumbling window state calculation and metrics rollups.
* **`GeoUtilsTest` & `DateTimeUtilsTest`:** Verifies geodesic Haversine distance and epoch window math.
* **`PaymentTransactionValidationTest`:** Verifies Jakarta Bean Validation constraints and defensive record boundaries.
* **`FraudAlertServiceTest`:** Mockito service test verifying business rule invariants during alert resolution.
* **`BatchAggregatorTest`:** Verifies Lambda batch reconciliation across date boundaries.
