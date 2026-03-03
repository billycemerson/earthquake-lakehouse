# Earthquake Lakehouse

A data engineering project that builds a simple lakehouse for earthquake data from BMKG (Indonesian Agency for Meteorology, Climatology, and Geophysics). The pipeline is orchestrated with Apache Airflow, runs in Docker, and stores data in DuckDB with bronze, silver, and gold layers.

## Project Overview

The pipeline executes every 10 minutes and follows a medallion architecture:

1. **Bronze** – Raw JSON data is fetched from the BMKG API and saved to `data/bronze/ingestion_date=YYYY-MM-DD/` without any transformation.
2. **Silver** – All bronze JSON files are read, flattened, cleaned (e.g., depth converted to float), and deduplicated using a hash of `DateTime` and `Coordinates`. The result is upserted into a DuckDB table `silver_earthquake`.
3. **Gold** – Aggregations are computed from the silver table:
   - Daily summary (event count, min/max/avg magnitude, min/max/avg depth)
   - Anomaly detection (events with magnitude > 8)
   Both are stored as DuckDB tables and exported as CSV files to `data/gold/`.

All data persists in the `data/` folder, which is mounted into the Airflow containers.

## Tech Stack

- **Apache Airflow** – Workflow orchestration
- **Docker & Docker Compose** – Containerization
- **DuckDB** – Embedded analytical database (lightweight warehouse)
- **Python** – ETL logic (requests, pandas)
- **PostgreSQL** – Airflow metadata database

## How to Run Locally

### Prerequisites
- Docker and Docker Compose installed
- Git (optional)

### Steps

1. **Clone the repository**
   ```bash
   git clone https://github.com/billycemerson/earthquake-lakehouse
   cd earthquake-lakehouse
   ```

2. **Create the required folders** (if not present)
   ```bash
   mkdir -p data/bronze data/silver data/gold logs plugins
   ```

3. **Set permissions** (Linux/Mac – ensures Airflow user can write to `data/`)
   ```bash
   sudo chown -R 50000:0 data
   sudo chmod -R 755 data
   ```

4. **Start the services**
   ```bash
   docker-compose up -d
   ```

5. **Access Airflow UI**
   Open [http://localhost:8080](http://localhost:8080)  
   Username: `admin`  
   Password: `admin`  

6. **Trigger the DAG** (or wait for the schedule)
   - In the Airflow UI, find the `earthquake_pipeline` DAG and toggle it on.
   - It will run automatically every 10 minutes.

7. **Check the results**
   - Bronze JSON files: `data/bronze/`
   - Silver table: query using DuckDB (`data/warehouse/bmkg.duckdb`)
   - Gold CSVs: `data/gold/daily_summary.csv` and `data/gold/anomalies.csv`

### Stopping the environment
```bash
docker-compose down
```
Add `-v` to also remove volumes (PostgreSQL data).

---