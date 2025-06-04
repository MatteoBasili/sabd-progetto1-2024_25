# 💡 SABD Project 1 – A.Y. 2024/2025

**Course:** Systems and Architectures for Big Data (SABD)  
**Team:** Matteo Basili, Adriano Trani  
**Professors:** Valeria Cardellini, Matteo Nardelli  
**Code and report submission:** June 9, 2025  
**Oral presentation:** June 19, 2025

---

## 📌 Project Objectives

The goal of the project is to process and analyze datasets on carbon intensity and renewable energy production in Italy and Sweden, using Big Data tools on distributed architectures. Queries are implemented with Apache Spark (using RDD API, DataFrame API, and SQL), and results are visualized and also evaluated from a performance perspective.

---

## 🛠️ Technology Stack

- **Apache Spark** (RDD API + DataFrame API + SQL, single-node cluster mode)  
- **Apache NiFi** (for data acquisition and ingestion)  
- **HDFS** (distributed data storage)  
- **Docker & Docker Compose** (containerization)  
- **Grafana** (results visualization)  
- **Redis** (for export)  
- **Python** (main programming language)

---

## 🧱 Architectural Diagram (TO DO)

[Add a .png image with link]

---

## 📁 Repository Structure

| Folder / File                    | Description                                                                 |
|---------------------------------|-----------------------------------------------------------------------------|
| `Report/`                       | Technical report in IEEE proceedings format                                |
| `Results/`                      | Query results in CSV format and charts                                     |
| `Results/analysis/`             | Experimental processing times                                              |
| `Results/csv/`                 | CSV output of queries Q1, Q2, Q3                                           |
| `Results/images/`              | Charts generated from query results                                       |
| `hdfs/`                         | Configuration and utilities for HDFS                                      |
| `nifi/`                         | Apache NiFi templates and utilities for data ingestion                     |
| `results_exporter/docker/`     | Dockerfile for exporting results from HDFS to Redis                       |
| `scripts/`                      | Scripts for ingestion, processing (Spark RDD/DataFrame/SQL), export, and charts |
| `specification/`                | Full project specification provided by professors                         |
| `docker-compose.yml`           | Complete cluster configuration (Spark, HDFS, NiFi, etc.)                  |

---

## ⚙️ Setup and Execution

### 🔧 Prerequisites

> ⚠️ The project runs **exclusively on Linux systems**.  
> ❌ Compatibility on Windows is not guaranteed.

Make sure you have the following installed:

- **Docker** ≥ 20.10  
- **Docker Compose** ≥ 1.29  
- [**Python**](https://www.python.org/) (recommended: version 3.8+)  
- **Selenium** version **4.6+** (requires Selenium Manager)  
- [**Google Chrome**](https://www.google.com/chrome/) (needed for Selenium)  

Install the necessary Python libraries with:

```bash
pip install requests selenium
pip install --upgrade requests urllib3 chardet
```

### 🚀 Start environment

```bash
git clone https://github.com/MatteoBasili/sabd-progetto1-2024_25.git
cd sabd-progetto1-2024_25
git checkout main
docker compose up -d
```

Access services at:
- **Apache NiFi UI:** http://localhost:8080/nifi
- **HDFS Web UI:** http://localhost:9870
- **Spark UI (job monitoring):** http://localhost:4040
- **Grafana UI (visualization):** http://localhost:3000

### 📦 Pipeline execution

Run the entire pipeline (from data ingestion to exporting results) using the script `run_full_pipeline.py`:
> 📂 **The script must be run from the project's root directory.**
```bash
python3 ./scripts/run_full_pipeline.py [q1|q2|q3] [rdd|df|sql]
```
- _q1_, _q2_, _q3_ indicate the query to run
- _rdd_, _df_, _sql_ specify the Spark API to use

The script automatically:
1. Starts the data acquisition and ingestion flow (NiFi)
2. Executes the query
3. Exports the results to Redis
4. Saves results as CSV in _Results/csv/_
5. Creates the charts

---

## 📊 Dataset

**Source:** [Electricity Maps](https://app.electricitymaps.com/)  
**Countries:** Italy, Sweden  
**Period:** 2021 – 2024  
**Granularity:** Hourly  
**Relevant fields:**
- `Carbon intensity gCO2eq/kWh (direct)`
- `Carbon-free energy percentage (CFE%)`

Data are loaded into HDFS both as CSV and Parquet after being pre-processed and converted into Parquet format via NiFi.

---

## 🔍 Query Descriptions
### 🔹 Q1 – Annual analysis by country

- Calculate mean, minimum, and maximum of carbon intensity and CFE percentage for each year (2021–2024)
- Generate comparison charts Italy vs. Sweden

### 🔹 Q2 – Monthly analysis (Italy only)

- Calculate monthly averages
- Top-5 rankings for metrics in ascending/descending order
- Charts for monthly variation

### 🔹 Q3 – Daily hourly analysis

- Aggregation by hourly time slot (0–23)
- Calculate percentiles (min, 25th, 50th, 75th, max)
- Hourly charts Italy vs. Sweden

---

## 📈 Performance Analysis
For each query, an experimental analysis of processing times was performed:
- **Evaluations:** mean and standard deviation over 10 runs
- **Controlled conditions:** no background processes, caching disabled between runs
- **Metrics collected:** directly from code
- **SQL vs API comparison:** Spark SQL times compared to RDD/DataFrame APIs in the report

---

## 📤 Output and Results

- All CSV results are in:
> 📂 _Results/csv/_
- Charts (from Grafana) are in:
> 📂 _Results/images_
- Statistical analysis of processing times is in:
> 📂 _Results/analysis_

---

## 📑 Documentation

- 📄 **Technical report**: `Report/sabd_report_basili_trani_2024_25.pdf` (IEEE format)
- 🖼️ **System architecture**: included in the report (PDF)

---

## 🤝 Contributors

- **Matteo Basili** – [GitHub Profile](https://github.com/MatteoBasili)
- **Adriano Trani** – [GitHub Profile](https://github.com/AdrianoTrani)

