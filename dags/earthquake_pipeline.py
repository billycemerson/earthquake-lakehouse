from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
from pathlib import Path
import requests
import json
import hashlib
import duckdb

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_bronze(**context):
    url = "https://data.bmkg.go.id/DataMKG/TEWS/gempadirasakan.json"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    ingestion_date = datetime.now().strftime("%Y-%m-%d")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    bronze_dir = Path("/opt/airflow/data/bronze") / f"ingestion_date={ingestion_date}"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    
    file_path = bronze_dir / f"earthquake_{timestamp}.json"
    with open(file_path, 'w') as f:
        json.dump(data, f)
    
    print(f"Saved raw data to {file_path}")
    context['ti'].xcom_push(key='bronze_file_path', value=str(file_path))

def bronze_to_silver(**context):
    bronze_path = Path("/opt/airflow/data/bronze")
    json_files = list(bronze_path.glob("**/*.json"))
    if not json_files:
        print("No JSON files found in bronze.")
        return
    
    db_path = "/opt/airflow/data/warehouse/bmkg.duckdb"
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(db_path)

    con.execute("""
    CREATE TABLE IF NOT EXISTS silver_earthquake (
        event_id VARCHAR PRIMARY KEY,
        datetime TIMESTAMPTZ,
        coordinates VARCHAR,
        magnitude DOUBLE,
        depth_km DOUBLE,
        wilayah VARCHAR,
        dirasakan VARCHAR,
        ingestion_time TIMESTAMPTZ,
        tanggal_original VARCHAR,
        jam_original VARCHAR
    )
    """)

    rows = []
    for file_path in json_files:
        with open(file_path, 'r') as f:
            data = json.load(f)
        gempa_list = data.get("Infogempa", {}).get("gempa", [])
        for row in gempa_list:
            event_hash = hashlib.md5(
                (row["DateTime"] + row["Coordinates"]).encode()
            ).hexdigest()
            depth = float(row["Kedalaman"].replace(" km", ""))
            dt = datetime.fromisoformat(row["DateTime"].replace('+00:00', '+00:00'))
            rows.append((
                event_hash,
                dt,
                row["Coordinates"],
                float(row["Magnitude"]),
                depth,
                row["Wilayah"],
                row["Dirasakan"],
                datetime.now(timezone.utc),
                row["Tanggal"],
                row["Jam"]
            ))

    con.executemany("""
    INSERT OR REPLACE INTO silver_earthquake
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)

    total = len(rows)
    con.close()
    print(f"Upserted {total} records to silver_earthquake.")

def silver_to_gold(**context):
    db_path = "/opt/airflow/data/warehouse/bmkg.duckdb"
    gold_path = Path("/opt/airflow/data/gold")
    gold_path.mkdir(parents=True, exist_ok=True)
    
    con = duckdb.connect(db_path)

    # Create daily summary table
    con.execute("""
    CREATE OR REPLACE TABLE gold_daily_summary AS
    SELECT 
        DATE(datetime) AS earthquake_date,
        COUNT(*) AS total_events,
        MIN(magnitude) AS min_magnitude,
        MAX(magnitude) AS max_magnitude,
        AVG(magnitude) AS avg_magnitude,
        MIN(depth_km) AS min_depth,
        MAX(depth_km) AS max_depth,
        AVG(depth_km) AS avg_depth
    FROM silver_earthquake
    GROUP BY DATE(datetime)
    ORDER BY earthquake_date DESC
    """)

    # Create anomaly table (magnitude > 8)
    con.execute("""
    CREATE OR REPLACE TABLE gold_anomalies AS
    SELECT 
        event_id,
        datetime,
        magnitude,
        depth_km,
        wilayah,
        dirasakan
    FROM silver_earthquake
    WHERE magnitude > 8
    ORDER BY datetime DESC
    """)

    # Export to CSV
    con.execute(f"COPY gold_daily_summary TO '{gold_path}/daily_summary.csv' (HEADER, DELIMITER ',')")
    con.execute(f"COPY gold_anomalies TO '{gold_path}/anomalies.csv' (HEADER, DELIMITER ',')")

    con.close()
    print("Gold tables created and CSV exported.")

with DAG(
    'earthquake_pipeline',
    default_args=default_args,
    description='Pipeline gempa dari BMKG',
    schedule_interval='*/10 * * * *',
    catchup=False,
    tags=['earthquake'],
) as dag:

    t1 = PythonOperator(
        task_id='fetch_bronze',
        python_callable=fetch_bronze,
    )

    t2 = PythonOperator(
        task_id='bronze_to_silver',
        python_callable=bronze_to_silver,
    )

    t3 = PythonOperator(
        task_id='silver_to_gold',
        python_callable=silver_to_gold,
    )

    t1 >> t2 >> t3