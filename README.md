# 📊 Global Health Data ETL Pipeline Using Google Cloud Platform & Apache Airflow

This project implements a **secure, scalable, end-to-end ELT pipeline** on **Google Cloud Platform (GCP)** to process a global health statistics CSV file containing over one million records.  
The pipeline ingests raw data from **Cloud Storage**, orchestrates transformations through **Apache Airflow**, structures the data in **BigQuery**, and prepares **country-specific datasets and views** for analytical consumption via **Looker**.

---

# 📌 Requirements

The Medical Research Team receives a **global health statistics CSV file** containing disease information for all countries.  
The system must ensure:

- Each country's Health Minister only accesses their **country-specific** medical data.  
- The ability to analyze diseases **without available treatment or vaccination**.
- Efficient handling of a CSV file with **over one million records**.

---

# ⚠️ Challenges

- The data is provided as a **single large CSV file** containing all countries.
- Due to the **confidential nature**, it cannot be shared with everyone.
- Analyzing such a large CSV directly is **inefficient and slow**.
- Analytical queries become difficult without structured datasets.

---

# 🎯 Objective

Develop a **robust data analytics solution** to securely manage and filter global health data, ensuring:

- Restricted access (country-level data visibility)
- Efficient exploration of diseases lacking treatment or vaccination
- Automated transformation of raw data into optimized reporting datasets

---

# 🏗️ Architecture Overview

This solution uses the following GCP components:

## **1️⃣ Cloud Storage**
Stores the raw global CSV uploaded by the research team.

## **2️⃣ Compute Engine (Apache Airflow)**
Airflow DAGs manage:

- File checks
- CSV ingestion into BigQuery
- Country-specific table creation
- Reporting view creation

## **3️⃣ BigQuery Data Warehouse**
Organized into:

- **Staging Dataset** → Raw ingested data  
- **Transform Dataset** → Country-wise tables  
- **Reporting Dataset** → Filtered analytical views  

## **4️⃣ Looker**
Provides dashboards for each country’s Health Minister.

---


