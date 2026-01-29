from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime
from dateutil.parser import isoparse
import psycopg2
import boto3
import csv
import json
import os
import uuid

POSTGRES_CONN = {
    "host": "postgres",
    "port": 5432,
    "dbname": "analytics",
    "user": "airflow",
    "password": "airflow"
}

S3_BUCKET = "landing"
S3_PREFIX = "raw_events"
PIPELINE_SOURCE = "postgres_trusted"
CHUNK_SIZE = 5000

@dag(
    dag_id="landing_to_trusted_events",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["trusted", "transform"]
)

def init():

    @task
    def discover_ingestions():
        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ["S3_ENDPOINT"],
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
        )

        paginator = s3.get_paginator("list_objects_v2")
        ingestion_ids = set()

        for page in paginator.paginate(
            Bucket=S3_BUCKET,
            Prefix=f"{S3_PREFIX}/"
        ):
            for obj in page.get("Contents", []):
                parts = obj["Key"].split("/")
                for p in parts:
                    if p.startswith("ingestion_id="):
                        ingestion_ids.add(p.split("=")[1])

        return list(ingestion_ids)


    @task
    def process_ingestion(ingestion_id: str):
        conn = psycopg2.connect(**POSTGRES_CONN)
        cursor = conn.cursor()

        # ---------- Idempotência ----------
        cursor.execute("""
            SELECT status
            FROM metadata.ingestion_control
            WHERE ingestion_id = %s and source_system='postgres_trusted'
        """, (ingestion_id,))
        row = cursor.fetchone()

        if row and row[0] == "SUCCESS":
            print(f"Ingestion {ingestion_id} already SUCCESS. Skipping.")
            conn.close()
            return

        if not row:
            cursor.execute("""
                INSERT INTO metadata.ingestion_control (
                    ingestion_id,
                    source_system,
                    status,
                    started_at
                )
                VALUES (%s, %s, 'PROCESSING', NOW())
            """, (ingestion_id, PIPELINE_SOURCE))
        else:
            cursor.execute("""
                UPDATE metadata.ingestion_control
                SET status = 'PROCESSING', started_at = NOW()
                WHERE ingestion_id = %s
            """, (ingestion_id,))
        conn.commit()

        # ---------- S3 ----------
        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ["S3_ENDPOINT"],
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
        )

        paginator = s3.get_paginator("list_objects_v2")

        records_read = 0
        records_written = 0
        business_sources = set()

        try:

            for page in paginator.paginate(
                Bucket=S3_BUCKET,
                Prefix=f"{S3_PREFIX}/"
            ):

                for obj in page.get("Contents", []):
                    key = obj["Key"]

                    if f"ingestion_id={ingestion_id}/" not in key:
                        continue

                    tmp_file = f"/tmp/{uuid.uuid4()}.csv"
                    s3.download_file(S3_BUCKET, obj["Key"], tmp_file)

                    with open(tmp_file) as f:
                        reader = csv.DictReader(f)

                        for row in reader:
                                records_read += 1
                                raw_payload = row["payload"]

                                # ---------- JSON malformatado
                                try:
                                    # Tentativa 1: JSON válido
                                    payload = json.loads(raw_payload)
                                except json.JSONDecodeError:
                                    try:
                                        # Tentativa 2: string Python dict → JSON 
                                        payload = json.loads(
                                            raw_payload
                                            .replace("'", '"')
                                            .replace("True", "true")
                                            .replace("False", "false")
                                        )
                                    except Exception:
                                        # Payload inválido 
                                        continue

                                event_type = payload.get("event_type")
                                event_ts = payload.get("event_timestamp")
                                source_system = row["source_system"]

                                if not event_type or not event_ts:
                                    continue

                                event_timestamp = isoparse(event_ts)
                                event_date = event_timestamp.date()

                                cursor.execute("""
                                    INSERT INTO trusted.events (
                                        ingestion_id,
                                        source_system,
                                        event_type,
                                        event_timestamp,
                                        event_date,
                                        user_id,
                                        success,
                                        payload
                                    )
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT DO NOTHING
                                """, (
                                    ingestion_id,
                                    source_system,
                                    event_type,
                                    event_timestamp,
                                    event_date,
                                    payload.get("user_id"),
                                    payload.get("success"),
                                    json.dumps(payload)
                                    ))

                                records_written += 1

                        os.remove(tmp_file)

            # ---------- Validação de consistência ----------
                cursor.execute("""
                    UPDATE metadata.ingestion_control
                    SET
                        status = 'SUCCESS',
                        records_read = %s,
                        records_written = %s,
                        finished_at = NOW()
                    WHERE ingestion_id = %s
                """, (records_read, records_written, ingestion_id))

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


    ingestion_ids = discover_ingestions()
    process_ingestion.expand(ingestion_id=ingestion_ids)

dags = init()