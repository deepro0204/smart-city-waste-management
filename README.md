# ♻ Smart City Waste Management System

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-4.2-black?logo=apachekafka)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-17-blue?logo=postgresql)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red?logo=streamlit)
![Status](https://img.shields.io/badge/Status-Complete-green)

A real-time IoT-based waste management system that monitors smart bins across a city, detects full bins, triggers alerts, and displays live insights on a dashboard.

---

## Problem Statement

Urban waste management is inefficient — garbage trucks follow fixed routes regardless of whether bins are full or empty. This wastes fuel, time, and resources. Smart cities need a data-driven solution that monitors bin fill levels in real time and optimizes collection routes.

---

## Solution

This system simulates 10 smart bins across a city. Each bin has a sensor that continuously reports its fill level. The data flows through a complete Data Engineering pipeline:
IoT Sensors → Kafka → Processing → PostgreSQL → Streamlit Dashboard

- Bins above **80% full** trigger automatic alerts
- City managers can see live bin status on a dashboard
- Battery levels are monitored for each bin
- Daily summaries are generated automatically

---

## Architecture

## Architecture

The system follows a complete Data Engineering pipeline:

**Step 1 — Data Generation**
IoT Sensor Simulator generates fill level data for 10 smart bins every 2 seconds.

**Step 2 — Real-time Ingestion**
Apache Kafka receives the sensor data through the `waste-bin-data` topic with 3 partitions.

**Step 3 — Stream Processing**
Kafka Consumer reads messages in real time, detects alerts, and saves to PostgreSQL.

**Step 4 — Batch Processing**
Spark-style processor performs aggregations — average, max, min fill levels per bin.

**Step 5 — Storage**
PostgreSQL stores all bin readings, alerts, and daily summaries in 3 tables.

**Step 6 — Visualization**
Streamlit dashboard displays live bin status, charts, and alerts.

> **Pipeline Flow:**
> `IoT Sensors` → `Apache Kafka` → `Kafka Consumer` → `PostgreSQL` → `Spark Processor` → `Streamlit Dashboard`

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Data Simulation | Python + Faker | Simulate IoT bin sensors |
| Data Ingestion | Apache Kafka 4.2 | Real-time streaming pipeline |
| Data Processing | PySpark / Pandas | Transform and analyze data |
| Data Storage | PostgreSQL 17 | Store readings, alerts, summaries |
| Orchestration | Python Scripts | Automate pipeline execution |
| Dashboard | Streamlit + Plotly | Live visualization |
| Language | Python 3.12 | Core programming language |

---

## Project Structure

## Project Structure

| File | Description |
|---|---|
| `scripts/sensor_simulator.py` | Simulates 10 IoT smart bin sensors |
| `scripts/data_analyzer.py` | Analyzes data using Pandas and NumPy |
| `scripts/db_loader.py` | Loads sensor data into PostgreSQL |
| `scripts/kafka_producer.py` | Sends bin data to Kafka topic |
| `scripts/kafka_consumer.py` | Reads from Kafka and saves to database |
| `scripts/spark_processor.py` | Spark-style aggregations and processing |
| `scripts/dashboard.py` | Live Streamlit dashboard |
| `data/` | Raw JSON sensor data files |
| `logs/` | Operation logs for all scripts |
| `requirements.txt` | Python dependencies |

---

## Features

- **Real-time Streaming** — Kafka producer sends bin data every 2 seconds
- **Automatic Alert Detection** — Bins above 80% fill level flagged instantly
- **Battery Monitoring** — Low battery warnings for each bin
- **Data Warehousing** — Daily summaries stored in PostgreSQL
- **Live Dashboard** — Color-coded bin status (Critical/Moderate/Normal)
- **Interactive Charts** — Fill level bar chart, status pie chart, battery chart
- **Log Management** — Every operation logged with timestamps

---

## Database Schema

### bin_readings
Stores every sensor reading from all bins.

### bin_alerts
Stores only the critical alerts (fill level > 80%).

### daily_summary
Aggregated daily statistics per bin — acts as a data warehouse table.

---

## Setup Instructions

### Prerequisites
- Python 3.12
- PostgreSQL 17
- Apache Kafka 4.2
- Java 22

### Installation

**1. Clone the repository**
```bash
git clone https://github.com/YOUR_USERNAME/smart-city-waste-management.git
cd smart-city-waste-management
```

**2. Create virtual environment**
```bash
python -m venv venv
venv\Scripts\activate
```

**3. Install dependencies**
```bash
pip install -r requirements.txt
```

**4. Setup PostgreSQL**
```bash
psql -U postgres
CREATE DATABASE smart_city_waste;
\c smart_city_waste
# Run the table creation SQL queries
```

**5. Start Kafka**
```bash
cd C:\kafka
bin\windows\kafka-server-start.bat config\server.properties
```

**6. Run the pipeline**
```bash
# Step 1 - Generate sensor data
python scripts/sensor_simulator.py

# Step 2 - Analyze data
python scripts/data_analyzer.py

# Step 3 - Load to database
python scripts/db_loader.py

# Step 4 - Start Kafka consumer (Window 1)
python scripts/kafka_consumer.py

# Step 5 - Start Kafka producer (Window 2)
python scripts/kafka_producer.py

# Step 6 - Process with Spark
python scripts/spark_processor.py

# Step 7 - Launch dashboard
streamlit run scripts/dashboard.py
```

---

## Data Engineering Concepts Covered

- **ETL Pipeline** — Extract from sensors, Transform with Pandas/Spark, Load to PostgreSQL
- **Real-time Streaming** — Apache Kafka producer-consumer architecture
- **Data Warehousing** — Star schema with fact and dimension tables
- **Batch Processing** — Historical data analysis with Pandas and NumPy
- **Stream Processing** — Live data ingestion via Kafka consumer
- **Data Quality** — Validation, alert detection, anomaly flagging
- **Orchestration** — Multi-script pipeline with logging

---

## Dashboard Preview

The live Streamlit dashboard shows:
- City overview metrics (total, critical, moderate, normal bins)
- Live bin status cards with color coding
- Fill level bar chart with alert threshold line
- Bin status distribution pie chart
- Recent alerts table
- Battery level monitoring chart

---

## Future Improvements

- Integrate Apache Airflow for pipeline orchestration
- Deploy on AWS EC2 with S3 data lake
- Add route optimization algorithm for garbage trucks
- Implement machine learning for fill level prediction
- Add email/SMS alert notifications
- Scale to 1000+ bins using cloud infrastructure

---

## Author

**Name:** Deepro Bhattacharyya  
**Roll Number:** 23051340  
**Batch/Program:** B.Tech CSE  
**Project Type:** Data Engineering Project

---

## License

This project is for educational purposes as part of a Data Engineering capstone submission.