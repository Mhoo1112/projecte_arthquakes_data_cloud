import os
import io
import pandas
import geopandas
import requests
import datetime
import sqlalchemy
from google.cloud import storage
from airflow.providers.postgres.hooks.postgres import PostgresHook


separator = "=" * 80
def load_data_from_gcs_to_db(bucket_name, TABLE_NAME, **kwargs):
    """อ่านข้อมูลจาก Google Cloud Storage และบันทึกไปที่ Database"""

    # --- ขั้นตอนการบันทึกข้อมูล ---

    # -- เชื่อมต่อกับ Google Cloud Storage --
    # สร้าง Object เพื่อเชื่อมต่อกับ Google Cloud
    storage_client = storage.Client()
    # กำหนด Bucket ภายใน Google Cloud Storage ด้วย Object Client
    # สร้าง Object เพื่อเชื่อมต่อกับ Bucket ใน Google Cloud 
    # >> bucket_name ค่าที่ส่งมาจากไฟล์ DAG
    bucket = storage_client.bucket(bucket_name)


    # -- ค้นหาไฟล์ล่าสุด --
    # -- ดึงข้อมูลจาก Task ก่อนหน้า --
    ti = kwargs['ti']
    procressed_data_blob_path = ti.xcom_pull(key='procressed_data_blob_path', task_ids='transfrom_data_from_gcs')


    # -- อ่านเนื้อหาภายในไฟล์ล่าสุด --
    # กำหนด ชื่อของ Blob ที่เป็นไฟล์ข้อมูลจาก Web API
    blob_object = bucket.blob(procressed_data_blob_path)
    # ดาวน์โหลดข้อมูลจาก [ไฟล์ที่บันทึกเป็น Bytes]
    parquet_bytes = blob_object.download_as_bytes()

    # parquet_bytes = blob_object.download_to_filename()
    # parquet_bytes = blob_object.download_as_text(encoding='utf-8')
    # processed_csv_data = io.StringIO(csv_string_data)

    processed_data = io.BytesIO(parquet_bytes)

    df_features = geopandas.read_parquet(processed_data)


    # -- เชื่อมต่อกับฐานข้อมูล --
    # กำหนดตัวแปรเฉพาะของฐานข้อมูล Postgre >> สร้าง Connection ไว้ที่ Airflow >> Admin >> Connection 
    pg_hook = PostgresHook(postgres_conn_id='postgres_cloud_sql')
    # สร้างตัวแปรเชื่อต่อกับฐานข้อมูล Postgre
    engine = pg_hook.get_sqlalchemy_engine()
    

    # กำหนดชื่อ ตาราง
    # >> TABLE_NAME ค่าที่ส่งมาจากไฟล์ DAG
    # TABLE_NAME = 'earthquakes'

    # เชื่อมต่อกับ Postgre ด้วยการใช้ Object engine 
    with engine.connect() as connection:


        # ติดตั้งส่วนเสริม PostGIS
        install_postgis_query = sqlalchemy.text(f"CREATE EXTENSION IF NOT EXISTS postgis;")
        connection.execute(install_postgis_query)


        # สร้างตารางและบันทึกข้อมูลลง PostgreSQL/PostGIS ด้วยคำสั่งเฉพาะ และใช้ engine ที่สร้างในการเชื่อมต่อ
        df_features.to_postgis(
            name = TABLE_NAME,
            con = engine,
            if_exists = 'append',
            index = False,
            # crs = df_features.crs.to_string(),
            dtype = {'geometry': 'GEOMETRY(Point, 4326)'}
        )

        # -- Query ข้อมูลทั้งหมด --
        select_table_query = sqlalchemy.text(f"""
            SELECT * FROM {TABLE_NAME} LIMIT 10;
            """)
        execute_select_table_query = connection.execute(select_table_query)

        # -- แสดงข้อมูล --  
        # ------------------------------------------------

        print(separator)
        print(f"--- ตัวอย่างข้อมูล ---")
        print(f"--- Query ---")
        print(f"{execute_select_table_query}")
        
        # ------------------------------------------------