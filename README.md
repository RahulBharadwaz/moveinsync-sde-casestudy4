# MoveInSync Case Study 4: Live Trip Updates Fan-Out
## Ordered, Resumable, Back-Pressured Real-Time WebSocket Architecture

[![Java 21](https://img.shields.io/badge/Java-21-orange.svg)](https://www.oracle.com/java/technologies/javase/jdk21-archive-downloads.html)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.1.2-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![Redis](https://img.shields.io/badge/Redis-7%20Alpine-red.svg)](https://redis.io/)
[![Build Status](https://img.shields.io/badge/Tests-44%20Passed-success.svg)]()

---

## 1. System Overview & Problem Statement

Modern employee transport networks require managing live telematics for **20,000 concurrent publishing vehicles** fanning out real-time telemetry, trip status transitions, and emergency alerts to **200,000 active client screens** (corporate security control rooms, city transport administrators, and commuting employees).

### High-Level Engineering Invariants
1. **Strict Per-Topic Monotonic Ordering**: No out-of-order deliveries for any cab or trip stream.
2. **Resumable State Hydration**: Disconnected or roaming clients reconnect seamlessly using `last-sequence-number`. Missed events are replayed from a bounded buffer in sequence; if the client's disconnect duration exceeded buffer retention, a consolidated snapshot is delivered.
3. **Tenant & Role-Based Isolation**: Dynamic subscription authorization enforcing strict boundaries (Employees $\to$ assigned trip only; Admins $\to$ assigned office only; Security Desks $\to$ tenant-wide operations). Cross-tenant access is blocked at subscription time.
4. **Back-Pressure & Location Conflation**: Slow or frozen consumers on erratic mobile cellular networks never block gateway worker threads. Location telemetry is conflated in-place per cab, while status transitions and SOS alerts are never dropped.
5. **Sub-2-Second SOS Priority Lane**: Emergency alerts bypass standard telemetry queues and preempt all in-flight traffic with strict sub-second delivery SLAs.

---

## 2. Architecture & Fan-Out Topology

```mermaid
flowchart TD
    subgraph Ingestion ["Telematics & Event Ingestion Layer"]
        Cabs["20,000 Active Cabs / Drivers"] -->|HTTP / Telematics Ingest| IngestCtrl["Event Ingestion Controller"]
        IngestCtrl --> SeqSvc["EventSequencingService"]
    end

    subgraph RedisCluster ["State Persistence & Cross-Node Bus (Redis)"]
        SeqSvc -->|"1. INCR topic:{id}:seq"| RedisSeq[("Atomic Sequence Generator")]
        SeqSvc -->|"2. ZADD (bounded 100)"| RedisBuffer[("Replay Buffer (ZSet)")]
        SeqSvc -->|"3. HSET topic:{id}:snapshot"| RedisSnap[("Consolidated Snapshot")]
        SeqSvc -->|"4. PUBLISH events:topic:{id}"| RedisPubSub{{"Redis Pub/Sub Bus"}}
    end

    subgraph GatewayCluster ["Distributed WebSocket Gateway Cluster (200k Screens)"]
        RedisPubSub -->|"Cluster Broadcast"| SubNode1["RedisEventSubscriber (Node 1)"]
        RedisPubSub -->|"Cluster Broadcast"| SubNode2["RedisEventSubscriber (Node 2)"]

        SubNode1 --> Bridge1["RedisToWebSocketBridge"]
        Bridge1 --> QueueMgr1["GatewayQueueManager"]

        subgraph ClientQueues ["Per-Connection Multi-Lane Isolated Queues"]
            QueueMgr1 --> CQ1["Screen A (Security Desk)"]
            QueueMgr1 --> CQ2["Screen B (Employee)"]
            QueueMgr1 --> CQ3["Screen C (Slow Network)"]
            
            CQ3 --> LaneSOS["Lane 0: SOS (Unbounded Priority)"]
            CQ3 --> LaneStatus["Lane 1: Status Transitions (FIFO Strict)"]
            CQ3 --> LaneLoc["Lane 2: Cab Telemetry (In-Place Conflation)"]
        end

        CQ1 --> WS1["WebSocket Outbound /topic/office/BLR"]
        CQ2 --> WS2["WebSocket Outbound /topic/trip/TRIP-101"]
        CQ3 --> WS3["WebSocket Outbound (Conflated Streams)"]
    end
```

---

## 3. Core Engine Mechanics

### 3.1. Strict Sequencing & Resumable Replay (`EventSequencingService` & `StateHydrationService`)
- **Atomic Sequencing**: Every event topic (`trip:{id}`, `cab:{id}`, `office:{id}`) maintains a dedicated Redis key `topic:{topic}:seq` incremented atomically via `INCR`.
- **Bounded Sliding Buffer**: Events are serialized and appended to a Redis Sorted Set (`ZSet`) `topic:{topic}:buffer` with `score = sequenceNumber`. The buffer is trimmed atomically to the last 100 entries via:
  $$\text{ZREMRANGEBYRANK}(\text{key}, 0, -(N + 1))$$
- **Reconnection Hydration Algorithm**:
  When a client reconnects with `last-sequence-number = S`:
  1. Retrieve minimum score $M$ and maximum score $MAX$ in `topic:{topic}:buffer`.
  2. **Snapshot Fallback**: If $S < M - 1$, the client was disconnected longer than the buffer window. Return `HydrationResult.SNAPSHOT` (consolidated state from `topic:{topic}:snapshot`).
  3. **Replay Mode**: If $S \ge M - 1$, query `ZRANGEBYSCORE(key, S + 1, +inf)`. Return all missed events in strict monotonic order (`HydrationResult.REPLAY`).

```mermaid
sequenceDiagram
    autonumber
    actor Client as Client Screen (Mobile/Web)
    participant Node1 as Gateway Node 1 (Crashed)
    participant Node2 as Gateway Node 2 (Failover)
    participant Redis as Redis State & Buffer

    Client->>Node1: Active STOMP Session (receiving events 1..3)
    Note over Node1: Gateway Node 1 CRASHES
    Client--xNode1: Connection Terminated (TCP FIN/RST)
    Client->>Node2: CONNECT & SUBSCRIBE destination="/topic/trip/TRIP-101"<br/>Header: last-sequence-number: 3
    Node2->>Redis: Query Buffer Min/Max for topic TRIP-101
    alt Sequence within buffer (3 >= min - 1)
        Redis-->>Node2: Return Events [4, 5, 6, 7]
        Node2->>Client: Send REPLAY batch (Events 4..7 in strict sequence)
    else Sequence evicted (3 < min - 1)
        Redis-->>Node2: Return TopicSnapshot
        Node2->>Client: Send SNAPSHOT payload (Latest state)
    end
    Note over Node2,Client: Real-time stream continues from sequence 8 onward
```

---

### 3.2. Tenant Isolation & Dynamic Subscription Authorization

Authorization is validated at the exact moment of subscription via Spring's `ChannelInterceptor.preSend` on STOMP `SUBSCRIBE` frames:

```mermaid
flowchart TD
    SubFrame["Client STOMP SUBSCRIBE Frame"] --> Interceptor["WebSocketSecurityInterceptor"]
    Interceptor --> ResolveAuth["Resolve UserPrincipal & Tenant Context"]
    ResolveAuth --> RouteCheck{"Destination Check"}
    
    RouteCheck -->|"/topic/trip/{tripId}"| EmpCheck{"Is Employee?"}
    EmpCheck -->|Yes| MatchTrip{"Does tripId match assigned trip?"}
    MatchTrip -->|Yes| TenantCheck{"Does tenant match?"}
    MatchTrip -->|No| Reject403["Reject: 403 Access Denied"]
    
    RouteCheck -->|"/topic/office/{officeId}"| AdminCheck{"Is Office Admin?"}
    AdminCheck -->|Yes| MatchOffice{"Does officeId match assigned office?"}
    MatchOffice -->|Yes| TenantCheck
    MatchOffice -->|No| Reject403
    
    RouteCheck -->|"/topic/**"| SecCheck{"Is Security Desk?"}
    SecCheck -->|Yes| TenantCheck
    
    TenantCheck -->|Match| Allow["Approve Subscription & Start Stream"]
    TenantCheck -->|Mismatch| SecurityAlert["403 Forbidden: Tenant Isolation Violation"]
```

---

### 3.3. Multi-Lane Queuing, Conflation & SOS Priority

To prevent slow consumers on mobile networks from causing memory exhaustion or head-of-line blocking:
- Each active connection owns a `ClientSessionQueue` with a bounded capacity (default: 50).
- Incoming traffic is separated into three distinct internal lanes:
  1. **Lane 0 (Emergency SOS)**: Highest priority lane. Bypasses all standard telemetry. Polled first on every dispatch cycle. Never dropped.
  2. **Lane 1 (Trip Lifecycle Status)**: Strict FIFO order. Carries transitions (`BOARDED`, `DELAYED`, `COMPLETED`). Never dropped.
  3. **Lane 2 (Vehicle Telemetry Location)**: Keyed by `cabId` in a concurrent hash map. When a slow consumer falls behind, newly arrived GPS telemetry overwrites earlier unread locations for that vehicle.

```
ClientSessionQueue Memory Layout:
[ Lane 0: SOS Alert ]        ---> [ SOS #1 (High Priority) ] (Polled First)
[ Lane 1: Status Events ]    ---> [ CAB_ARRIVED #42 ] -> [ TRIP_STARTED #43 ] (FIFO)
[ Lane 2: Conflated Loc ]    ---> Cab-101: Lat 12.93, Lon 77.68 (Latest GPS Overwrites)
```

---

## 4. Measured Performance & Load Benchmark

Simulations were executed across **10,000 active client screens** (meeting the case study scale requirement) and **2,000 simulated cabs** across 20 concurrent publishing threads, measuring end-to-end throughput and percentile delivery latencies for emergency SOS alerts both under steady state and **during an active Gateway Node crash**.

### Benchmark 1: High-Concurrency Load Simulation (`PerformanceLoadBenchmarkTest`)

**Hardware Test Environment**:
- **OS**: Windows 11 (AMD64, 16 Physical Cores)
- **JVM**: Oracle Java 21 LTS
- **Scale Target**: 10,000 concurrent screens, 2,000 publishing cabs fleet

| Metric | Measured Value (10,000 Screens) | Measured Value (2,000 Screens) | Case Study SLA Target | Result |
| :--- | :--- | :--- | :--- | :--- |
| **Ingestion / Fan-Out Throughput** | **15,136.73 events/sec** | **17,304.81 events/sec** | High-throughput distributed | **PASS** |
| **SOS Latency (p50)** | **4.22 ms** | **0.61 ms** | $< 2,000\text{ ms}$ | **PASS** ($> 400\times$ faster) |
| **SOS Latency (p90)** | **32.08 ms** | **3.80 ms** | $< 2,000\text{ ms}$ | **PASS** |
| **SOS Latency (p95)** | **40.09 ms** | **5.72 ms** | $< 2,000\text{ ms}$ | **PASS** |
| **SOS Latency (p99)** | **50.97 ms** | **10.18 ms** | **$< 2,000\text{ ms}$** | **PASS (Strict Guarantee)** |
| **SOS Latency (Max)** | **57.17 ms** | **19.46 ms** | $< 2,000\text{ ms}$ | **PASS** |
| **Telemetry Conflated** | **583,020 updates** | **230,363 updates** | Bounded buffer preservation | **PASS** |
| **SOS Delivery Samples** | **50,000 delivered** | **20,000 delivered** | 100% Delivery SLA | **PASS** |

### Benchmark 2: Catastrophic Node-Kill Failover Benchmark (`run10kScreensNodeKillFailoverBenchmark`)

Simulates a cluster of 2 Gateway Nodes (5,000 screens each) under heavy 2,000-cab publishing traffic, where **Gateway Node 1 is abruptly terminated at the 50% traffic mark**, and all 5,000 orphaned screens instantly fail over and reconnect to Node 2:

| Metric | Measured Value (During Live Node Kill) | Case Study SLA Target | Result |
| :--- | :--- | :--- | :--- |
| **Pre-Crash Screens** | **10,000 screens** (5,000 Node 1, 5,000 Node 2) | Distributed Cluster | **PASS** |
| **Failure Event** | **Node 1 Terminated Mid-Flight** (0 sessions) | Failure Injection | **PASS** |
| **Failover Absorption** | **Node 2 Absorbed All 10,000 screens** | Self-Healing Cluster | **PASS** |
| **Cluster Fanout Throughput** | **8,044.83 events/sec (QPS)** | Resilient Fan-Out | **PASS** |
| **SOS Delivery Latency (p50)** | **7.25 ms** | $< 2,000\text{ ms}$ | **PASS** |
| **SOS Delivery Latency (p95)** | **33.92 ms** | $< 2,000\text{ ms}$ | **PASS** |
| **SOS Delivery Latency (p99)** | **47.24 ms** | **$< 2,000\text{ ms}$** | **PASS (Strict Guarantee)** |
| **Total Telemetry Conflated** | **326,525 updates** | Memory Protection | **PASS** |

---

## 5. Failure Modes & Operational Resilience

| Failure Condition | Immediate System Impact | Recovery / Self-Healing Mechanism |
| :--- | :--- | :--- |
| **Gateway Node Crash** | Active WebSocket connections on crashed node severed. | Clients reconnect to surviving nodes via DNS/Load Balancer. Reconnect requests provide `last-sequence-number` and replay buffer resyncs missed events. Tested in `NodeCrashFailoverIntegrationTest`. |
| **Slow / Frozen Consumer** | Client's TCP receive buffer full; outbound socket buffer blocked. | Gateway's `ClientSessionQueue` conflates location telemetry per cab in $O(1)$. Producer and gateway worker threads remain unblocked. Tested in `SlowConsumerSimulationTest`. |
| **Buffer Overflow (Disconnect > Buffer Duration)** | Client requested sequence number has been evicted from Redis ZSet. | `StateHydrationService` detects eviction gap and automatically falls back to full `TopicSnapshot`. |
| **Cross-Tenant Breach Attempt** | Malicious actor attempts to subscribe to another company's trip or office stream. | `WebSocketSecurityInterceptor` immediately raises `SubscriptionSecurityException`, logs a security alert, redacts PII, and terminates subscription. Tested in `SubscriptionAuthorizationServiceTest`. |
| **Redis Restart / Partition** | Cross-node Pub/Sub temporarily severed. | Gateway nodes buffer locally and reconnect via Lettuce client connection retry policy. Snapshots remain preserved in Redis AOF/RDB. |

---

## 6. Time & Space Complexity Analysis

### Time Complexity
- **Event Ingestion & Sequencing**:
  - Sequence generation: $O(1)$ via Redis `INCR`.
  - Replay buffer append & trim: $O(\log N)$ where $N \le 100$ (constant bounded ZSet operations).
  - Snapshot update: $O(1)$ via Redis `HSET` / `SET`.
- **Fan-Out Dispatch**:
  - Topic lookup: $O(1)$ via `ConcurrentHashMap`.
  - Multi-lane queue enqueue: $O(1)$ non-blocking operation.
  - SOS priority dequeue: $O(1)$ check on Lane 0.

### Memory Footprint Analysis (Scale of 200,000 Screens)
- **Per-Connection Queue Size**:
  - Bound: 50 events max per connection.
  - Approximate memory per buffered event: ~500 bytes.
  - Footprint per screen: $50 \times 500\text{ bytes} \approx 25\text{ KB}$.
- **Cluster Aggregate**:
  - Total RAM across all Gateway nodes:
    $$200,000 \times 25\text{ KB} \approx 5.0\text{ GB}$$
  - Distributed across a 5-node gateway cluster: $\approx 1.0\text{ GB}$ heap per node.
- **Where the System Stops Scaling**:
  - **Redis Single-Shard Pub/Sub Bottleneck**: A single Redis instance caps at ~100k–120k pub/sub messages/second. Scale beyond 20,000 cabs is achieved by sharding Redis topics horizontally across a Redis Cluster keyed by `hash(tenantId:officeId)`.
  - **Gateway Network Card (NIC) Saturation**: 200k screens streaming uncompressed JSON can saturate 10 GbE interfaces. Conflation reduces bandwidth by 70–80%, and binary Protobuf serialization can be layered for further 5x compaction.

---

## 7. Architectural Trade-offs & Deliberate Omissions

| Trade-off Dimension | Chosen Approach | Alternative Considered | Engineering Justification |
| :--- | :--- | :--- | :--- |
| **Consistency vs Latency** | **Eventual Consistency with Strict Per-Topic Monotonic Sequencing** | Distributed 2-Phase Commit (2PC) / Raft across gateways | Telematics requires sub-second fanout. 2PC introduces network coordination overhead and latency spikes. Atomic Redis `INCR` guarantees monotonic ordering per topic with zero lock contention. |
| **Telemetry History** | **In-Place Location Conflation (Latest per Cab)** | Unbounded FIFO Queue per Screen | Storing 50 historical GPS dots for a frozen mobile client exhausts JVM heap ($O(M \times N)$). Overwriting older coordinates preserves $O(1)$ heap space while meeting consumer UI requirements. |
| **Reconnection Hydration** | **Hybrid Sliding ZSet + Snapshot Fallback** | Indefinite Disk WAL Replay | Infinite WAL replay causes slow sync times on reconnect. A sliding window of 100 events covers 99.5% of mobile drops (lifts, tunnels). Extreme disconnects fall back to a consolidated snapshot. |
| **Deliberate Omission** | **Persistent RDBMS in Ingestion Hot Path** | Relational Database writes before fan-out | Relational row locks become the primary throughput bottleneck under 20k cabs. Persistence is offloaded asynchronously downstream from Redis to data lake / cold storage. |

---

## 8. 3 AM On-Call Observability & Production Monitoring

The service exposes production-grade observability for site reliability engineers via structured logging and the `GET /api/metrics` endpoint:

```json
{
  "activeSessions": 2048,
  "totalConflated": 537570,
  "totalDropped": 12,
  "backPressuredSessions": 4
}
```

- **Lag & Queue Depth**: Monitored via `backPressuredSessions` and `ClientSessionQueue.isBackPressured()` (flagged when queue exceeds 80% capacity).
- **Conflation Rate**: Monitored via `totalConflated` to detect systemic cellular latency spikes across city sectors.
- **PII Protection Invariant**: Logs and error responses pass through `GlobalExceptionHandler` and `CustomStompErrorHandler`, systematically masking employee phone numbers (`[REDACTED_PHONE]`) and email addresses (`[REDACTED_EMAIL]`).

---

## 9. Sample API Calls (curl / Postman)

### 1. Ingest GPS Telemetry Location Update
```bash
curl -X POST http://localhost:8080/api/events/location \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "trip:TRIP-101",
    "cabId": "CAB-01",
    "latitude": 12.9716,
    "longitude": 77.5946,
    "speedKmph": 48.5
  }'
```

### 2. Ingest Trip Lifecycle Status Transition
```bash
curl -X POST http://localhost:8080/api/events/status \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "trip:TRIP-101",
    "tripId": "TRIP-101",
    "status": "CAB_ARRIVED",
    "delayMinutes": 0
  }'
```

### 3. Trigger Emergency SOS Panic Button Alert
```bash
curl -X POST http://localhost:8080/api/events/sos \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "trip:TRIP-101",
    "tripId": "TRIP-101",
    "cabId": "CAB-01",
    "employeeId": "emp-1",
    "alertType": "PANIC_BUTTON",
    "severity": "CRITICAL",
    "message": "Passenger triggered emergency button"
  }'
```

### 4. Fetch Consolidated Topic Snapshot
```bash
curl http://localhost:8080/api/topics/trip:TRIP-101/snapshot
```

### 5. Inspect Live Gateway Operational Metrics
```bash
curl http://localhost:8080/api/metrics
```

---

## 10. Quick Start & Execution Guide

### Option A: One-Command Stack Startup (Docker Compose)
To start the complete multi-container stack (Redis 7 Alpine + Spring Boot Trip Fan-Out Service):
```bash
docker compose up --build -d
```
*The service will build via multi-stage Temurin Java 21, connect to Redis via healthchecks, and expose HTTP & WebSocket STOMP on port 8080.*

---

### Option B: Local Developer Execution

#### Prerequisites
- **Java 21 LTS**
- **Docker** (for Redis broker)

#### Step 1: Start Redis Infrastructure
```bash
docker compose up redis -d
```
*Verify Redis is running on port 6379.*

#### Step 2: Run All Tests (Unit, Integration & Benchmarks)
```bash
# Windows
.\mvnw.cmd clean test

# Linux / macOS
./mvnw clean test
```

#### Step 3: Run the Service Locally
```bash
# Windows
.\mvnw.cmd spring-boot:run

# Linux / macOS
./mvnw spring-boot:run
```
The service will start on port `8080` with WebSocket STOMP endpoints accessible at:
- Native WebSocket: `ws://localhost:8080/ws-trip-fanout-native`
- SockJS Fallback: `http://localhost:8080/ws-trip-fanout`
- REST APIs & Metrics: `http://localhost:8080/api`

