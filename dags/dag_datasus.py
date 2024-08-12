from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import minio
from datasus import read_dbc
import pandas as pd
import os

# Configuration for MinIO
MINIO_ENDPOINT = "minio:9000"  # Replace with your MinIO endpoint
MINIO_ACCESS_KEY = "minioadmin"  # Replace with your MinIO access key
MINIO_SECRET_KEY = "minioadmin"  # Replace with your MinIO secret key
MINIO_BUCKET_NAME = "raw"  # Replace with your MinIO bucket name
MINIO_DATA_SUS_FOLDER = "datasus/dbc"  # Folder within the bucket for DataSUS files

CLIENT = minio.Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False  # Adjust based on your MinIO configuration
)

dag = DAG(
    dag_id = 'ingest_transform_datasus_data',
    description="Download dbc files from DataSUS ftp and transform csv files.",
    schedule_interval='@monthly',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'start_date': datetime(2024, 8, 6),
        'catchup': False,
    },
    tags=['datasus']
)


ingest_task = BashOperator(
    task_id='ingest',
    bash_command="python /home/airflow/pysus/ingest.py",  # noqa: E501
    dag=dag,
)


def load_dbc(obj):
    df = pd.DataFrame(read_dbc(obj.object_name))
    return df


def dbc_csv(minio_client=CLIENT, raw_bucket=MINIO_BUCKET_NAME):
    """
    Converts .dbc files in the specified raw bucket in MinIO to .csv files in the specified csv bucket.

    Args:
        minio_client (minio.Minio): A MinIO client object.
        raw_bucket (str): Name of the raw bucket in MinIO.
        csv_bucket (str): Name of the bucket in MinIO where .csv files will be saved.
    """
    objects = minio_client.list_objects(raw_bucket, prefix="datasus/dbc/", recursive=True)
    for obj in objects:
        if obj.object_name.endswith('.dbc'):
            # Download the file from MinIO
            minio_client.fget_object(raw_bucket, obj.object_name, obj.object_name)
            
            # Read the file and convert to CSV
            df = load_dbc(obj)
            csv_filename = obj.object_name.replace('.dbc', '.csv')
            csv_filepath = os.path.join(csv_filename)  # Temporary file
            df.to_csv(csv_filepath, index=False)
            
            # Upload the CSV file to MinIO in the specified path
            csv_path_in_bucket = os.path.join("/datasus/csv", csv_filename)
            minio_client.fput_object(raw_bucket, csv_path_in_bucket, csv_filepath)
            
            # Delete the temporary file
            os.remove(csv_filepath)
            
            print(f"Converted {obj.object_name} to {csv_filename} and moved to {raw_bucket}/datasus/csv/")

transform_task = PythonOperator(
    task_id='transform',
    python_callable=dbc_csv,
    provide_context=True,
    dag=dag
)


ingest_task >> transform_task