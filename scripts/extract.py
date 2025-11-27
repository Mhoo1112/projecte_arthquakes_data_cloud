import os
import io
import pandas
import geopandas
import requests
import datetime
from google.cloud import storage


separator = "=" * 80
def extract_web_api(url, bucket_name, destination_raw_path, **kwargs):
    """ดึ่งข้อมูลจาก Web Api บันทึกไปที่ Google Cloud Storage"""

    try:
        # --- ขั้นตอนการดึงข้อมูล ----  
        # ดึงข้อมูลจาก Web Api
        # >> url ค่าที่ส่งมาจากไฟล์ DAG
        url_data = requests.get(url)

        # แปลง Text String เป็น Json [ ตัวแปร url_data ]
        raw_data = url_data.json()

        # ดึงข้อมูลในส่วนของ Key ชื่อ Features
        raw_data_features = raw_data.get('features', []) # หากไม่มี features จะคืนค่าเป็น List เปล่า []

        # แปลงเป็น GeoDataframe
        df_features = geopandas.GeoDataFrame.from_features(raw_data_features)


        # -- แสดงข้อมูล --  
        # ------------------------------------------------

        print(separator)
        print(f"--- ดึงข้อมูลแผ่นดินไหวจาก Web API ---")
        print(f"URL : {url}")
        print(f"จำนวนของเหตุการณ์ {len(raw_data_features)}") 

        # ------------------------------------------------

        print(separator)
        print(f"--- ตัวอย่างข้อมูล GeoDataFrame ---")
        print(f"{df_features.head(5)}")

        # ------------------------------------------------

        print(separator)
        print(f"--- โครงสร้าง GeoDataFrame ---")
        print(f"{df_features.info()}")

        # ------------------------------------------------


        # --- ขั้นตอนการบันทึกข้อมูล ----
        # กำหนดวันที่และเวลา
        timestamp = datetime.datetime.now().strftime("%Y-%m-%dTT%H%M%S")
        
        # กำหนดชื่อไฟล์
        filename = f'{timestamp}_raw_earthquake'

        # สร้าง Object เพื่อเชื่อมต่อกับ Google Cloud
        storage_client = storage.Client()

        # กำหนด Bucket ภายใน Google Cloud Storage ด้วย Object Client
        # สร้าง Object เพื่อเชื่อมต่อกับ Bucket ใน Google Cloud 
        # >> bucket_name ค่าที่ส่งมาจากไฟล์ DAG
        bucket = storage_client.bucket(bucket_name)


        
        # กำหนด "ไฟล์จำลอง" ภายใน RAM
        csv_buffer = io.StringIO()

        # บันทึกข้อมูล Dataframe จากตัวแปร >> df_features << ไปที่ ไฟล์จำลอง
        df_features.to_csv(csv_buffer, index=False)

        # ดึงเนื้อหาทั้งหมดที่อยู่ใน buffer
        csv_content = csv_buffer.getvalue() # อ้างอิงจาก >> io.StringIO() คืนค่าเป็น Str

        # กำหนด Blob ปลายทางของ .csv
        # >> destination_raw_path ค่าที่ส่งมาจากไฟล์ DAG
        csv_full_path = os.path.join(destination_raw_path, f"{filename}.csv")
        csv_blob = bucket.blob(csv_full_path) # << อ้างอิงจาก path ทั้งหมดที่สร้าง

        # อัปโหลดข้อมูล String ไปที่ GCS โดยตรง
        csv_blob.upload_from_string(csv_content, content_type='text/csv')


        # กำหนด "ไฟล์จำลอง" ภายใน RAM
        parquet_buffer = io.BytesIO()
        # บันทึกข้อมูล Dataframe จากตัวแปร >> df_features << ไปที่ ไฟล์จำลอง
        df_features.to_parquet(parquet_buffer, index=False)
        # ดึงเนื้อหาทั้งหมดที่อยู่ใน buffer
        parquet_content = parquet_buffer.getvalue() # อ้างอิงจาก >> io.BytesIO() คืนค่าเป็น byte
        # กำหนด Blob ปลายทางของของ .parquet
        # >> destination_raw_path ค่าที่ส่งมาจากไฟล์ DAG
        parquet_full_path = os.path.join(destination_raw_path, f"{filename}.parquet")
        parquet_blob = bucket.blob(parquet_full_path) # << อ้างอิงจาก path ทั้งหมดที่สร้าง
        # อัปโหลดข้อมูล Bytes ไปที่ GCS โดยตรง
        parquet_blob.upload_from_bytes(parquet_content, content_type='application/octet-stream')


        # -- แสดงข้อมูล --
        # ------------------------------------------------

        print(separator)
        print(f"--- บันทึก GeoDataFrame ---")
        print(f"BUCKET : {bucket_name}")
        print(f"DESTINATION : {os.path.basename(destination_raw_path)}")
        print(f"FILE NAME : {filename}")

        # ------------------------------------------------

        # -- ส่งค่าไฟยัง Task ถัดไป --
        ti = kwargs['ti']
        ti.xcom_push(key='raw_data_blob_path', value=parquet_full_path)
    
    
    except Exception as e:
        print(separator)
        print(f"เกิดข้อผิดพลาดที่ : {e}")
        print(separator)
        raise