# JMS vs Kafka — Throughput & Latency Benchmark

A comparative performance lab measuring producer/consumer throughput and median latency between **Apache ActiveMQ (JMS)** and **Apache Kafka**, using multi-threaded Java clients and Docker-based infrastructure.

![Java 17+](https://img.shields.io/badge/Java-17%2B-blue)
![Kafka](https://img.shields.io/badge/Apache-Kafka-green)
![ActiveMQ](https://img.shields.io/badge/ActiveMQ-Classic-orange)
![Docker](https://img.shields.io/badge/Docker-Compose-teal)

---

## Benchmark Results

Results measured over 10,000 messages on a shared topic/queue named `barcelona`.

| Metric | JMS (ActiveMQ) | Kafka |
|---|---|---|
| Producer throughput | 8,000 msgs/sec | **150,000 msgs/sec** |
| Consumer throughput | 16,000 msgs/sec | **196,001 msgs/sec** |
| Producer response time | 1 ms | 1 ms |
| Consumer response time | 0 ms | 0 ms |
| Median latency | 1 ms | **1 ms** |

> Kafka delivers **~18× higher producer throughput** and **~12× higher consumer throughput** while matching JMS on latency.

---

##  Project Structure
src/main/java/org/example/

├── JMSProducer.java              # JMS single-threaded producer + latency test

├── JMSConsumer.java              # JMS single-threaded consumer + latency test

├── JMSThroughput.java            # JMS multi-threaded producer throughput test

├── JMSConsumerThroughput.java    # JMS multi-threaded consumer throughput test

├── ProducerClass.java            # Kafka producer + latency test

└── ConsumerClass.java            # Kafka consumer + latency test

src/main/resources/

└── message.txt                   # Payload used in all tests

docker-compose.yml                # Kafka + Zookeeper + ActiveMQ


---

##  Getting Started

### Prerequisites
- Java 17+
- Maven or Gradle
- Docker & Docker Compose

### 1. Start Infrastructure

```bash
docker compose up -d
```

Services started:
- **Kafka** on `localhost:9092`
- **Zookeeper** on `localhost:2181`
- **ActiveMQ** on `localhost:61616` (broker) and `localhost:8161` (web console — admin/admin)

### 2. Run JMS Benchmarks

```bash
# Multi-threaded producer throughput ramp
JMSThroughput.main()

# Multi-threaded consumer throughput ramp
JMSConsumerThroughput.main()

# Single-threaded latency baseline
JMSProducer.main()   # send 10k messages
JMSConsumer.main()   # receive and measure latency
```

### 3. Run Kafka Benchmarks

```bash
ProducerClass.main()   # produce 10k messages, report median response
ConsumerClass.main()   # consume and report median latency
```

---

## Methodology

- **Throughput ramp**: starts at 1,000 msgs/sec, doubles each round. A round fails if >10% of messages miss their deadline window. The last passing rate is reported as max throughput.
- **Latency measurement**: a Unix timestamp is embedded as a prefix in each message payload (`timestamp|body`). The consumer calculates end-to-end latency on receipt.
- **Thread synchronisation**: a `CyclicBarrier` ensures all threads start simultaneously for fair load distribution.
- **Median**: computed by sorting response/latency lists and taking the midpoint value.

---

## Key Takeaways

| Scenario | Recommendation |
|---|---|
| High-volume event streaming | ✅ Kafka |
| Transactional point-to-point messaging | ✅ ActiveMQ JMS |
| Sub-millisecond latency at any scale | ✅ Both |
| Simple setup, fewer moving parts | ✅ ActiveMQ JMS |

