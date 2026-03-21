# 🚀 YouTube Trending Data Engineering Pipeline (Streaming)

## 📌 Overview

This project implements an **end-to-end real-time data engineering pipeline** that ingests trending YouTube video data, processes it using distributed streaming, and generates analytics for identifying viral content, SEO patterns, and content strategies.

The pipeline is built using **Azure-native services + modern data engineering practices**, following the **Medallion Architecture (Bronze → Silver → Gold)**.

---

## 🏗️ Architecture

* **Data Source** → YouTube Data API
* **Streaming Layer** → Azure Event Hub (Kafka API)
* **Processing Engine** → Azure Databricks (Structured Streaming)
* **Storage** → Azure Data Lake Storage Gen2 (Delta Lake)
* **Orchestration** → Apache Airflow
* **Containerization** → Docker

---

## 🔄 Pipeline Flow

```text
YouTube API → Producer → Event Hub (Kafka)
            ↓
      Databricks (Streaming)
            ↓
   Bronze → Silver → Gold (Delta Lake)
            ↓
        Analytics / Insights
            ↓
         Airflow Orchestration
```

---

## 📁 Project Structure

```text
youtube-trending-streaming-pipeline/
│
├── producer/                  # Data ingestion (YouTube → Event Hub)
├── databricks/               # Streaming + transformations
│   ├── bronze_layer.py
│   ├── silver_layer.py
│   ├── Gold_Layer.py
│   └── Datavalidation.py
│
├── airflow/                  # DAG for orchestration
│   └── youtube_pipeline_dag.py
│
├── docker/                   # Airflow local setup
│   └── docker-compose.yaml
│
├── config/                   # Environment configs
│   └── .env.example
│
├── docs/                     # Architecture diagrams (optional)
├── requirements.txt
├── README.md
└── .gitignore
```

---

## 🧱 Medallion Architecture

### 🥉 Bronze Layer (Raw Ingestion)

* Reads streaming data from Event Hub (Kafka)
* Parses JSON messages
* Stores raw data in Delta Lake
* Implements checkpointing for fault tolerance

---

### 🥈 Silver Layer (Data Cleaning & Validation)

* Removes null/invalid records
* Converts timestamps
* Deduplicates records
* Applies data validation rules
* Writes clean data to Delta tables

---

### 🥇 Gold Layer (Analytics & Insights)

* Computes **trend score**
* Extracts **keywords (SEO signals)**
* Generates **hashtag insights**
* Aggregates metrics for viral content analysis

---

## ⚙️ Key Features

* ✅ Real-time streaming ingestion
* ✅ Delta Lake (ACID + scalable storage)
* ✅ Structured Streaming with checkpointing
* ✅ Data validation layer (quality checks)
* ✅ Medallion architecture implementation
* ✅ Airflow orchestration (job scheduling)
* ✅ Dockerized Airflow setup
* ✅ Cloud-native (Azure) design

---

## 📊 Example Analytics (Gold Layer)

* Trending videos by engagement
* Keyword frequency from titles
* Hashtag/SEO insights
* Viral content scoring

---

## 🛠️ Tech Stack

* **Python**
* **PySpark**
* **Azure Event Hub (Kafka API)**
* **Azure Databricks**
* **ADLS Gen2**
* **Delta Lake**
* **Apache Airflow**
* **Docker**

---

## ▶️ How to Run

### 1. Start Producer

```bash
python producer/youtube_trending_producer.py
```

---

### 2. Run Databricks Pipeline

Execute in order:

1. Bronze Layer
2. Data Validation
3. Silver Layer
4. Gold Layer

---

### 3. Run Airflow (Local)

```bash
cd docker
docker-compose up
```

Access UI:

```text
http://localhost:8080
```

Login:

```text
admin / admin
```

---

### 4. Configure Databricks in Airflow

Add connection in Airflow UI:

* **Conn Id**: `databricks_default`
* **Host**: your Databricks workspace URL
* **Token**: Personal Access Token

---

## 🔐 Configuration

All environment variables are stored in:

```text
config/.env.example
```

Create your own `.env` file locally and never commit secrets.

---

## 📈 Future Improvements

* Add NLP-based keyword extraction (KeyBERT / spaCy)
* Integrate Great Expectations for data quality
* Use Databricks Job Clusters (cost optimization)
* Add dashboarding (Power BI / Tableau)
* Implement CI/CD pipeline

---

## 🧠 Key Learnings

* Designing real-time data pipelines using Kafka-based streaming
* Implementing Medallion Architecture with Delta Lake
* Handling streaming checkpoints and fault tolerance
* Orchestrating pipelines with Airflow
* Managing cloud storage securely using RBAC

---

## 💼 Resume Highlight

> Built a real-time YouTube analytics pipeline using Azure Event Hub and Databricks, implementing Delta Lake Medallion architecture and orchestrating workflows using Apache Airflow.

---

## ⭐ Author

**Sreerag S**

---

## 📌 Notes

This project is designed as a **production-style data engineering system** and can be extended for real-world analytics use cases.

---
