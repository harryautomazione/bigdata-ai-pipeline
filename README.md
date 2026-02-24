⚙️ Objective:
This project implements an end-to-end real-time fraud detection pipeline using:

Apache Kafka for event streaming

Spark Structured Streaming for real-time processing

Event-time windowing with watermarking

Spark ML Pipeline (Logistic Regression) for fraud prediction

Kafka-to-Kafka architecture for scalable downstream consumption

The system processes transaction events in real time, computes user-level spending features over sliding windows, applies a trained ML model, and publishes fraud predictions to a downstream Kafka topic.

🏗 Architecture:
                ┌────────────────────┐
                │  Transaction       │
                │  Producer          │
                │  (generate_data.py)│
                └─────────┬──────────┘
                          │
                          ▼
                ┌────────────────────┐
                │  Kafka Topic       │
                │  transaction_events│
                └─────────┬──────────┘
                          │
                          ▼
                ┌────────────────────────────┐
                │ Spark Structured Streaming │
                │                            │
                │ • Event-time parsing       │
                │ • Watermark (10 min)       │
                │ • 30s window aggregation   │
                │ • Feature engineering      │
                │ • ML Pipeline inference    │
                └─────────┬──────────────────┘
                          │
                          ▼
                ┌────────────────────┐
                │ Kafka Topic        │
                │ fraud_predictions  │
                └─────────┬──────────┘
                          │
                          ▼
                ┌────────────────────┐
                │ Kafka Consumer     │
                │ (real-time output) │
                └────────────────────┘
⚙️ Key Features:

Event-time window aggregation (30 seconds)

Watermarking (10 minutes) for late data handling

Stateful processing using Spark Structured Streaming

ML model trained using Spark ML Pipeline

Kafka-to-Kafka streaming architecture

Checkpointing for fault tolerance

⚙️ ML Model:

Logistic Regression

Features:

total_spend

transaction_count

avg_amount

Pipeline-based training (VectorAssembler + LR)

▶️ How to Run:

1️⃣ Start Kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

2️⃣ Create Topics
bin/kafka-topics.sh --create --topic transaction_events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic fraud_predictions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

3️⃣ Train Model
python train_model.py

4️⃣ Start Streaming Job
python streaming_fraud_scoring.py

5️⃣ Start Producer
python generate_data.py

6️⃣ Consume Predictions
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic fraud_predictions --from-beginning

⚙️ This project demonstrates:

Distributed stream processing

Stateful event-time computation

Real-time ML inference

Kafka-based event-driven architecture

Production-style streaming pipeline
