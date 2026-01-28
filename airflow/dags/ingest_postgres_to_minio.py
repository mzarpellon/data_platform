from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import psycopg2
import boto3
import csv
import os 
import uuid
from datetime import datetime 

# =========================
# Configurações
# =========================
POSTGRES_CONN = {
    "host": "postgres",
    "port": 5432,
    "dbname": "analytics",
    "user": "airflow",
    "password": "airflow"
}

S3_BUCKET = "landing"
S3_PREFIX = "raw_events"
CHUNK_SIZE = 10000

LOCAL_TZ = "America/Sao_Paulo"

def extract_and_load():
    ingestion_id = str(uuid.uuid4())
    source_system = "postgres_landing"

    conn = psycopg2.connect(**POSTGRES_CONN)
    cursor = conn.cursor()

    # =========================
    # Registrar início
    # =========================
    cursor.execute("""
    INSERT INTO metadata.ingestion_control (
        ingestion_id,
        source_system,
        status,
        started_at
    )
    VALUES (%s, %s, 'STARTED', NOW())
    """, (ingestion_id, source_system))
    conn.commit()

    # =========================
    # Buscar dados
    # =========================
    cursor.execute("""
    SELECT
        id,
        source_system,
        ingestion_id,
        payload,
        file_name,
        ingested_at
    FROM landing.raw_events
    WHERE ingestion_id = %s
    ORDER BY id
    """, (ingestion_id,))

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    ingestion_date = datetime.utcnow().date().isoformat()
    part = 0
    records_read = 0
    records_written = 0

    try:
        while True:
            rows = cursor.fetchmany(CHUNK_SIZE)
            if not rows:
                print('Nows rows to be processed.')
                break 
            
            part += 1
            records_read += len(rows)

            filename = f"/tmp/part-{part}.csv"

            with open(filename, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "id",
                    "source_system",
                    "ingestion_id",
                    "payload",
                    "file_name",
                    "ingested_at"
                ])
                writer.writerows(rows)
            
            s3_key = (
                f"{S3_PREFIX}/"
                f"ingestion_date={ingestion_date}/"
                f"ingestion_id={ingestion_id}/"
                f"part-{part}.csv"
            )

            s3.upload_file(filename, S3_BUCKET, s3_key)
            os.remove(filename)

            records_written += len(rows)

        # =========================
        # Finalizar com sucesso
        # =========================
        cursor.execute("""
            UPDATE metadata.ingestion_control
            SET
                status = 'SUCCESS',
                records_read = %s,
                records_written = %s,
                finished_at = NOW()
            WHERE ingestion_id = %s
        """(records_read, records_written, ingestion_id))
        conn.commit()
    except Exception as e:
        cursor.execute("""
            UPDATE metadata.ingestion_control
            SET
                status = 'FAILED',
                error_message = %s,
                finished_at = NOW()
            WHERE ingestion_id = %s
        """, (str(e), ingestion_id))
        conn.commit()
        raise 

with DAG(
    dag_id="ingest_postgres_to_minio_landing",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["landing", "postgres", "minio"]
) as dag:

    ingest_task = PythonOperator(
        task_id="extract_postgres_to_minio", 
        python_callable=extract_and_load,
        provide_context=True
    )
