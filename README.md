# Retail Data Pipeline: Real Time & Batch Processing
This project aims to build an end-to-end pipeline designed to process retail store transactions.
It implements a **Medallion Architecture** (Bronze, Silver and Gold level) handling both real-time streaming data for responsive live visualizations and batch processing for daily aggregations.

## 🏗️ Architecture & Tech Stack
- Data Generation: python script `generator.py` simulating real-time checkout receipts;
- Message Broker: *Apache Kafka*, used for streaming data ingestion;
- Process Engine: *Apache Spark*, used for both Structured Streaming and Batch processing;
- Orchestration: *Apache Airflow*, used to execute the Batch processing;
- Data Lake: *MinIO*, used for DataLake tables and DLQ to simulate AWS S3;
- Data Warehouse: *ClickHouse*, OLP database used to retrieve data in a faster way for visualization;
- Visualization: *Grafana*, used for real-time and static monitoring and analytics dashboards.

## 📂 Project Structure
```
.
├── dags/
│   └── batch_dag.py              # Airflow DAG for daily batch orchestration
├── data/                         # Static datasets used for data enrichment (stores, items, checkouts) and logs saving last receipt of each store/checkout
├── grafana/                      # Provisioning files for reconstructing Grafana dashboards
├── scripts/
│   ├── generator.py              # Simulates live retail transactions
│   ├── streaming_processor.py    # Spark Structured Streaming (Bronze -> Silver -> Gold)
│   └── batch_processor.py        # Spark Batch processing for daily analytics
├── sql/
│   └── clickhouse_tables.sql     # DDL schemas for ClickHouse tables
├── .env.example                  # Example for environment variables (used to access to the various services)
├── docker-compose.yaml           # Infrastructure setup (Airflow, Spark, Kafka, ClickHouse, Grafana)
├── Dockerfile                    # Custom Airflow image with Java & PySpark dependencies
└── environment.yml               # Conda/Python environment definition
```

## 🚀 Pipeline Workflow

#### 1. Data Generation (`generator.py`)
Simulates retail environments by generating continuos receipt data across different stores and checkouts. This data is then pushed directly to Apache Kafka topics.
#### 2. Streaming Processing (`streaming_processor.py`)
Consumes data from Kafka in real-time and applies the Medallion Architecture:
- Bronze: raw data ingested directly from Kafka;
- Silver: cleaned, filtered and enriched data;
- Gold (Real-Time): highly aggregated data used for immediate visualization. This data is written directly to ClickHouse to visualize live Grafana dashboards,
#### 3. Batch Processing (`batch_processor.py`, `batch_dag.py`)
Scheduled and orchestrated by Airflow, the batch pipeline run on a daily schedule during the night. it processes historical Silver data to calculate daily metrics and long-term trends for the decision making of the company.

## 💡 Architectural Decisions
To ensure the pipeline is robust, scalable, and highly performant, several specific design choices were made:
- **ClickHouse over Traditional RDBMS**: ClickHouse was selected as the Data Warehouse for the Gold layer. As a columnar database, it provides vastly superior performance for heavy aggregation queries over large data volumes compared to traditional relational databases like PostgreSQL or MySQL.
- **S3-backed Checkpointing (MinIO)**: To guarantee exactly-once semantics and complete system resilience in case of container crashes, Spark Structured Streaming checkpoints are safely stored in MinIO (S3).
- **Dead Letter Queue (DLQ)**: To prevent pipeline bottlenecks or crashes caused by malformed messages, corrupted records are dynamically filtered and routed to a DLQ. These records are saved in JSON format on S3 (MinIO) for future debugging and data quality analysis.
- **Native ClickHouse Spark Connector**: Instead of relying on a standard, slower JDBC connection, the pipeline utilizes the native clickhouse-spark-runtime. This drastically optimizes write throughput and reduces latency when pushing streaming aggregations to the Gold layer.

## 📊 Dashboards (Grafana)
The project includes a pre-configured Grafana dashboard (`retail_dashboard.json`) connected to ClickHouse, it feautures:
- Real-time monitoring of active checkouts / stores;
- Real-time monitoring of articles sale;
- Breakdowns of payment methods;
