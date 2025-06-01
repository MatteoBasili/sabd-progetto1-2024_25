# 💡 SABD Progetto 1 – A.A. 2024/2025

**Corso:** Sistemi e Architetture per Big Data (SABD)  
**Team:** Matteo Basili, Adriano Trani 
**Docenti:** Valeria Cardellini, Matteo Nardelli  
**Consegna codice:** 9 giugno 2025  
**Presentazione orale:** 19 giugno 2025

---

## 📌 Obiettivi del progetto

L'obiettivo del progetto è elaborare e analizzare dataset sull'intensità di carbonio e sulla produzione di energia rinnovabile in Italia e Svezia, utilizzando strumenti Big Data su architetture distribuite. Le query vengono implementate con Apache Spark (usando sia RDD API, sia DataFrame API, sia SQL), e i risultati vengono visualizzati e valutati anche dal punto di vista delle prestazioni.

---

## 🛠️ Stack tecnologico

- **Apache Spark** (RDD API + DataFrame API + SQL, modalità cluster su singolo nodo)
- **Apache NiFi** (per acquisizione e ingestion dei dati)
- **HDFS** (storage distribuito dei dati)
- **Docker & Docker Compose** (containerizzazione)
- **Grafana** (visualizzazione risultati)
- **Redis** (per l'esportazione)
- **Python** (linguaggio principale)

---

## 🧱 Schema architetturale (DA FARE)

[Mettere un'immagine .png con link]

---

## 📁 Struttura del repository (DA FARE)

├── hdfs/ # Dati in input/output su HDFS
├── nifi/ # Template e configurazioni NiFi per l'ingestion
├── results_exporter/ # Esportazione dei risultati da HDFS (es. verso Redis)
├── scripts/ # Script Spark per Q1, Q2, Q3
├── specification/ # Documentazione del progetto
│ ├── Report/ # Relazione in formato IEEE
│ └── Results/ # Output CSV delle query
├── docker-compose.yml # Configurazione cluster (Spark, NiFi, ecc.)

---

## ⚙️ Setup ed esecuzione (DA FARE)

### 🔧 Prerequisiti

- Docker ≥ 20.10  
- Docker Compose ≥ 1.29  
- Python ≥ 3.8 (opzionale, per test locali)

### 🚀 Avvio ambiente

????????????????????```bash
git clone https://github.com/MatteoBasili/sabd-progetto1-2024_25.git
cd sabd-progetto1-2024_25
git checkout spark-q3
docker-compose up --build

Accedi ai servizi:
- **Apache NiFi UI:** http://localhost:8080/nifi
- **HDFS Web UI:** http://localhost:9870
- **Spark UI (job monitoring):** http://localhost:4040
- **Grafana UI (visualizzazione):** http://localhost:3000

---

## 📊 Dataset
**Fonte:** [Electricity Maps](https://app.electricitymaps.com/)  
**Nazioni:** Italia, Svezia  
**Periodo:** 2021 – 2024  
**Granularità:** Oraria  
**Campi rilevanti:**
- `Carbon intensity gCO2eq/kWh (direct)`
- `Carbon-free energy percentage (CFE%)`

I dati vengono caricati in HDFS sia in versione CSV che in versione Parquet dopo essere stati pre-processati e convertiti in formato Parquet tramite NiFi.

---

## 🔍 Descrizione delle Query
### 🔹 Q1 – Analisi annuale per nazione

- Calcolo media, minimo e massimo di intensità carbonica e percentuale CFE per ogni anno (2021–2024)
- Generazione grafici di confronto Italia vs. Svezia

### 🔹 Q2 – Analisi mensile (solo Italia)

- Calcolo medie mensili
- Classifiche top-5 per metrica in ordine crescente/decrescente
- Grafici per variazione mensile

### 🔹 Q3 – Analisi oraria giornaliera

- Aggregazione per fascia oraria (0–23)
- Calcolo percentili (min, 25°, 50°, 75°, max)
- Grafici orari per Italia vs. Svezia

---

## 📈 Analisi delle prestazioni
Per ogni query è stata effettuata un'analisi sperimentale dei tempi di processamento:
- **Valutazioni:** media e deviazione standard su 10 esecuzioni
- **Condizioni controllate:** nessun processo in background, caching disabilitato tra esecuzioni
- **Metriche rilevate:** direttamente dal codice
- **Confronto SQL vs API:** i tempi Spark SQL sono confrontati con quelli RDD/DataFrame nel report

---

## 📤 Output e risultati

- Tutti i risultati in formato CSV si trovano in:
> 📂 _Results/csv/_
- I grafici (da Grafana) si trovano in:
> 📂 _Results/images_
- Le analisi statistiche sui tempi di processamento si trovano in:
> 📂 _Results/analysis_

---

## 📑 Documentazione

- 📄 **Relazione tecnica**: `Report/sabd_report_basili_trani_2024_25.pdf` (formato IEEE)
- 🖼️ **Architettura del sistema**: inclusa nel report (PDF)

---

## 🤝 Collaboratori

- **Matteo Basili** – [GitHub Profile](https://github.com/MatteoBasili)
- **Adriano Trani** – [GitHub Profile](https://github.com/AdrianoTrani)

