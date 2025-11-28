from plugins.extract import extract_web_api
from plugins.transfrom import transfrom_data_from_gcs
from plugins.load import load_data_from_gcs_to_db

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime



with DAG(
    dag_id="etl_earthquakes_api",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["etl"]
) as dag:


    BUCKET_NAME = Variable.get("GCS_BUCKET_NAME", default_var= f'earthquake-data-lake-123456')
    URL = r'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson'
    DESTINATION_RAW_PATH = Variable.get("RAW_PATH", default_var= f'data/raw/')
    DESTINATION_PROCRESSED_PATH = Variable.get("PROCRESSED_PATH", default_var= f'data/processed/')
    TABLE_NAME = 'earthquakes'


    t1 = PythonOperator(
        task_id="extract_web_api",
        python_callable=extract_web_api,
        op_kwargs={
            'url' : URL,
            'bucket_name': BUCKET_NAME,
            'destination_raw_path': DESTINATION_RAW_PATH
        }
    )


    t2 = PythonOperator(
        task_id="transfrom_data_from_gcs",
        python_callable=transfrom_data_from_gcs,
        op_kwargs={
            'bucket_name': BUCKET_NAME,
            'destination_processed_path': DESTINATION_PROCRESSED_PATH
        }
    )

    
    t3 = PythonOperator(
        task_id="load_data_from_gcs_to_db",
        python_callable=load_data_from_gcs_to_db,
        op_kwargs={
            'bucket_name': BUCKET_NAME,
            'TABLE_NAME': TABLE_NAME
        }
    )

    t1 >> t2 >> t3
    # .