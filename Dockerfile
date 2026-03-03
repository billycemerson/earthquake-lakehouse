FROM apache/airflow:2.10.2-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
WORKDIR /opt/airflow

# Salin file requirements dan install dependencies
COPY --chown=airflow:airflow requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Salin kode proyek
COPY --chown=airflow:airflow dags/ dags/
COPY --chown=airflow:airflow data/ data/
COPY --chown=airflow:airflow scripts/ scripts/