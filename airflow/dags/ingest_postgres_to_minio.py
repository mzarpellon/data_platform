from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime
import psycopg2
import boto3
import csv
import os
import uuid

POSTGRES_CONN = {
    "host": "postgres",
    "port": 5432,
    "dbname": "analytics",
    "user": "airflow",
    "password": "airflow",
}

S3_BUCKET = "landing"
S3_PREFIX = "raw_events"
CHUNK_SIZE = 5000


@dag(
    dag_id="postgres_landing_to_minio",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["ingestion", "landing"],
)
def ingestion_dag():

    # -------------------------------------------------
    # Descobrir ingestion_ids pendentes
    # -------------------------------------------------
    @task
    def discover_pending_ingestions():
        conn = psycopg2.connect(**POSTGRES_CONN)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT DISTINCT l.ingestion_id
            FROM landing.raw_events l
            LEFT JOIN metadata.ingestion_control m
              ON l.ingestion_id = m.ingestion_id
            WHERE m.ingestion_id IS NULL
        """)

        ingestion_ids = [row[0] for row in cursor.fetchall()]
        conn.close()

        return ingestion_ids

    # -------------------------------------------------
    # processar UMA ingestão
    # -------------------------------------------------
    @task
    def process_ingestion(ingestion_id: str):
        source_system = "postgres_landing"

        conn = psycopg2.connect(**POSTGRES_CONN)
        cursor = conn.cursor()

        # Registrar início
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

        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ["S3_ENDPOINT"],
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        )

        ingestion_date = datetime.utcnow().date().isoformat()
        part = 0
        records_read = 0

        try:
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

            while True:
                rows = cursor.fetchmany(CHUNK_SIZE)
                if not rows:
                    break

                part += 1
                records_read += len(rows)

                filename = f"/tmp/{ingestion_id}-part-{part}.csv"

                with open(filename, "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        "id",
                        "source_system",
                        "ingestion_id",
                        "payload",
                        "file_name",
                        "ingested_at",
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

            # Finalizar com sucesso
            cursor.execute("""
                UPDATE metadata.ingestion_control
                SET
                    status = 'SUCCESS',
                    records_read = %s,
                    records_written = %s,
                    finished_at = NOW()
                WHERE ingestion_id = %s
            """, (records_read, records_read, ingestion_id))
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
        finally:
            conn.close()

    # -------------------------------------------------
    # Orquestração
    # -------------------------------------------------
    ingestion_ids = discover_pending_ingestions()
    process_ingestion.expand(ingestion_id=ingestion_ids)


dag = ingestion_dag()
