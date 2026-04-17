# Retail Data Pipeline: Real Time & Batch Processing
This project aims to build an end-to-end pipeline designed to process real-time retail store transactions.
It implements a **Medallion Architecture** (Bronze, Silver and Gold level) handling both real-time streaming data for responsive live visualizations and batch processing for daily aggregations required to calculates vital business metrics, allowing stakeholders to monitor store health.

## 🏗️ Architecture & Tech Stack
- **Data Generation**: python script `generator.py` simulating real-time checkout receipts;
- **Message Broker**: *Apache Kafka*, used for streaming data ingestion;
- **Process Engine**: *Apache Spark*, used for both Structured Streaming and Batch processing;
- **Orchestration**: *Apache Airflow*, used to execute the Batch processing;
- **Data Lake**: *MinIO*, used for DataLake tables and DLQ to simulate AWS S3;
- **Data Warehouse**: *ClickHouse*, OLP database used to retrieve data in a faster way for visualization;
- **Visualization**: *Grafana*, used for real-time and static monitoring and analytics dashboards.

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
- **Fault-Tolerance & Exactly-Once Processing**: Engineered a robust Lambda Architecture ensuring zero data loss. Implemented Spark Structured Streaming checkpoints backed by MinIO (S3) and strictly managed Kafka offsets to guarantee exactly-once processing semantics, even in the event of container or worker crashes.
- **ClickHouse over Traditional RDBMS**: Initially considered standard RDBMS like PostgreSQL for the serving layer. However, given the high throughput of streaming data and the requirement for heavy, on-the-fly analytical aggregations, I opted for ClickHouse. As a columnar OLAP database, ClickHouse drastically reduced query latency, making it the perfect fit for the Gold-level serving layer powering Grafana.
- **S3-backed Checkpointing (MinIO)**: To guarantee exactly-once semantics and complete system resilience in case of container crashes, Spark Structured Streaming checkpoints are safely stored in MinIO (S3).
- **Dead Letter Queue (DLQ)**: To prevent pipeline bottlenecks or crashes caused by malformed messages, corrupted records are dynamically filtered and routed to a DLQ. These records are saved in JSON format on S3 (MinIO) for future debugging and data quality analysis.
- **Native ClickHouse Spark Connector**: Instead of relying on a standard, slower JDBC connection, the pipeline utilizes the native clickhouse-spark-runtime. This drastically optimizes write throughput and reduces latency when pushing streaming aggregations to the Gold layer.

## 📊 Dashboards (Grafana)
The project includes a pre-configured Grafana dashboard (`retail_dashboard.json`) connected to ClickHouse, it feautures:
- Real-time monitoring of active checkouts / stores;
- Real-time monitoring of articles sale;
- Breakdowns of payment methods;

## 🛠️ Setup & Usage
1. **Clone the Repository**: `git clone https://github.com/Pizz0x/Retail-Data-Pipeline.git`
2. **Set Environment Variables**: set the environment variables using `.env.example` by modifying the missing field with the desired values
3. **Start the Infrastructure**: build the custom Airflow image and start Kafka, MinIO, ClickHouse, Airflow and Grafana, `docker compose up --build -d`
4. **Initialize the Data Lake (MinIO)**: access the MinIO Console at `http://localhost:9001` (Check `.env` for credentials), navigate to "Buckets" and manually create a new bucket named `retail.data`.
5. **Initialize the Data Warehouse (ClickHouse)**: execute the SQL schemas to create the necessary tables in ClickHouse, `cat sql/clickhouse_tables.sql | docker exec -i clickhouse-gold clickhouse-client`
6. **Run the Pipeline**:
    - Start the data generator: `python3 scripts/generator.py --store Store_Name --checkout --Checkout_Number`
    - Start the streaming processor: `python3 scripts/streaming_processor.py`
    - Access the Airflow interface in `localhost:8080`, according to the declared value, to enable and monitor `batch_dag`
    - Access the Grafana interface in `localhost:3001` to view the real-time dashboard. The dashboard and ClickHouse connections are automatically provisioned and ready to use!

