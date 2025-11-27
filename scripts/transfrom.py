import os
import io
import pandas
import geopandas
import requests
import datetime
from google.cloud import storage


separator = "=" * 80
def transfrom_data_from_gcs(bucket_name, destination_processed_path, **kwargs):
    """อ่านข้อมูลจาก Google Cloud Storage และทำความสะอาดข้อมูล"""

    # --- ขั้นตอนการทำความสะอาดข้อมูล ---

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
    raw_data_blob_path = ti.xcom_pull(key='raw_data_blob_path', task_ids='extract_web_api')


    # -- อ่านเนื้อหาภายในไฟล์ล่าสุด --
    # กำหนด ชื่อของ Blob ที่เป็นไฟล์ข้อมูลจาก Web API
    blob_object = bucket.blob(raw_data_blob_path)
    # ดาวน์โหลดข้อมูลจาก [ไฟล์ที่บันทึกเป็น Bytes]
    parquet_bytes = blob_object.download_as_bytes()

    # parquet_bytes = blob_object.download_to_filename()
    # parquet_bytes = blob_object.download_as_text(encoding='utf-8')
    # raw_csv_data = io.StringIO(csv_string_data)

    raw_data = io.BytesIO(parquet_bytes)

    df_features = geopandas.read_parquet(raw_data)



    # -- แสดงข้อมูล -- 
    # ------------------------------------------------

    print(separator)
    print(f"--- ตัวอย่างข้อมูล GeoDataFrame ---")
    print(f"BUCKER : {bucket_name}")
    print(f"FILE : {os.path.basename(raw_data_blob_path)}")
    print(f"{df_features.head(5)}")

    # ------------------------------------------------

    print(separator)
    print(f"--- โครงสร้าง GeoDataFrame ---")
    print(f"{df_features.info()}")

    # ------------------------------------------------

    
    # -- การทำความสะอาดข้อมูล และเลือกข้อมูลที่ต้องการ -- 
    # เปลี่ยนชื่อคอลัมน์
    df_features['magtype'] = df_features['magType']
    # df_features = df_features.rename(columns={"magType": "magtype"})

    # เลือกคอลัมน์ที่จะลบ
    columns_to_drop = ['magType']
    df_features = df_features.drop(columns_to_drop, axis=1)


    # เพิ่มคอลัมน์ time_add โดยการทำให้ข้อมูลเป็นข้อมูลเวลาในรูปแบบ int64 และเป็นเวลาในโซน UTC
    df_features['time_add'] = int(datetime.datetime.utcnow().timestamp()*1000)


    # แปลงชนิดของข้อมูล nst dmin gap
    df_features['nst'] = pandas.to_numeric(df_features['nst'], errors='coerce')
    df_features['nst'] = df_features['nst'].fillna(0).astype('int64')

    df_features['dmin'] = pandas.to_numeric(df_features['dmin'], errors='coerce')
    df_features['dmin'] = df_features['dmin'].fillna(0).astype('float64')
    
    df_features['gap'] = pandas.to_numeric(df_features['gap'], errors='coerce')
    df_features['gap'] = df_features['gap'].fillna(0).astype('float64')

    df_features = df_features.drop_duplicates(inplace=False)
    
    
    # แปลงข้อมูล geometry เป็นระบบพิกัด EPSG:4326
    TARGET_CRS = 'EPSG:4326'
    if df_features.crs is None:
        print("CRS ยังไม่ได้ถูกตั้งค่า กำลังตั้งค่าเป็น EPSG:4326")
        df_features = df_features.set_crs(TARGET_CRS, allow_override=True)
    elif df_features.crs != TARGET_CRS:
        print(f"CRS ปัจจุบันคือ {df_features.crs.to_string()} กำลังแปลงเป็น EPSG:4326")
        df_features = df_features.to_crs(TARGET_CRS)
    else:
        print(f"CRS ถูกต้องแล้ว ({TARGET_CRS}) ไม่ต้องแปลงเป็น EPSG:4326")
    
    print(f"ระบบพิกัดปัจจุบัน คือ: {df_features.crs}")

    select_column = ['geometry',
            'mag',
            'place',
            'time',
            'updated',
            'url',
            'detail',
            'status',
            'tsunami',
            'sig',
            'ids',
            'sources',
            'nst',
            'dmin',
            'rms',
            'gap',
            'magtype',
            'type',
            'time_add']

    df_features_selected_column = df_features[select_column]


    # -- การบันทึกไฟล์ --
    # กำหนดวันที่และเวลา
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dTT%H%M%S") 
    
    # กำหนดชื่อไฟล์
    filename = f'{timestamp}_processed_earthquake'


    # กำหนด "ไฟล์จำลอง" ภายใน RAM
    parquet_buffer = io.BytesIO()
    # บันทึกข้อมูล Dataframe จากตัวแปร >> df_features_selected_column << ไปที่ ไฟล์จำลอง
    df_features_selected_column.to_parquet(parquet_buffer, index=False)
    # ดึงเนื้อหาทั้งหมดที่อยู่ใน buffer
    parquet_content = parquet_buffer.getvalue() # อ้างอิงจาก >> io.BytesIO() คืนค่าเป็น byte
    # กำหนด Blob ปลายทางของของ .parquet
    # >> destination_processed_path ค่าที่ส่งมาจากไฟล์ DAG
    parquet_full_path = os.path.join(destination_processed_path, f"{filename}.parquet")
    parquet_blob = bucket.blob(parquet_full_path) # << อ้างอิงจาก path ทั้งหมดที่สร้าง
    # อัปโหลดข้อมูล Bytes ไปที่ GCS โดยตรง
    parquet_blob.upload_from_bytes(parquet_content, content_type='application/octet-stream')

    
    # -- แสดงข้อมูล --
    # ------------------------------------------------

    print(separator)
    print(f"--- บันทึก GeoDataFrame ---")
    print(f"BUCKET : {bucket_name}")
    print(f"DESTINATION : {os.path.basename(destination_processed_path)}")
    print(f"FILE NAME : {filename}")

    # ------------------------------------------------


    # -- ส่งค่าไฟยัง Task ถัดไป --
    ti.xcom_push(key='procressed_data_blob_path', value=parquet_full_path)