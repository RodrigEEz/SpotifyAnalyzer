# Spotify Data Engineering Project

This project implements a **modern data engineering pipeline** that collects, processes, and analyzes Spotify user data using a modular and scalable architecture. It integrates OAuth2 authentication, orchestration with Airflow, data processing with Spark, and transformation with dbt following the **Medallion Architecture**.  

---

## 🚀 Project Overview

- **Authentication Server (FastAPI/Flask)**  
  Handles Spotify OAuth2 flow, manages user registration, and securely stores refresh tokens.  

- **Airflow (Orchestration)**  
  Orchestrates the data pipeline: extraction, transformation, and loading into the data warehouse.  

- **Spark (Processing)**  
  Processes raw Spotify data, performs transformations, and prepares data for the silver layer.  

- **dbt (Modeling)**  
  Applies business logic and manages the Medallion architecture (Bronze → Silver → Gold).  

- **Database**  
  Stores authentication tokens and provides the warehouse backend for dbt.  

- **Visualization (optional)**  
  Insights can be consumed with BI tools such as Metabase, Superset, or Power BI.  

---

## 🛠️ Tech Stack

- **Languages**: Python, SQL  
- **Frameworks & Tools**: Flask, SQLAlchemy  
- **Data Orchestration**: Apache Airflow  
- **Data Processing**: Apache Spark  
- **Data Modeling**: dbt  
- **Database**: PostgreSQL (can be extended to Snowflake, BigQuery, etc.)  
- **Storage**: MinIO (S3-compatible) or cloud object storage  
- **Containerization**: Docker & Docker Compose  