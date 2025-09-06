
## 📌 Overview
This project implements an **ETL pipeline** that extracts data from an API, stores it in **MinIO** (S3-compatible object storage), transforms it using **Python**, and loads the processed data into **PostgreSQL**. The pipeline is orchestrated by **Apache Airflow** running on **Docker**, and the results are visualized with **Metabase**.

## ⚙️ Architecture
The pipeline follows these steps:
1. **Extract**: Fetch data from API and store raw files in MinIO.
2. **Transform**: Clean and process the data using Python.
3. **Load**: Insert the transformed data into PostgreSQL.
4. **Visualization**: Use Metabase to query PostgreSQL and build dashboards.
5. **Orchestration**: Airflow manages the ETL workflow and scheduling.

![Architecture Diagram](images/architecture.png)
