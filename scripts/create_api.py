from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from geoalchemy2 import Geometry
from typing import List, Optional
import sqlalchemy
import os
import json
import datetime
from dotenv import load_dotenv


SEPARATOR = "=" * 80
SUB_SEPARATOR = "-" * 80
# ----- create api -----
import sqlalchemy

# สร้าง class ของ API
class Mag_earthquakes(BaseModel):
    hour_of_day: datetime.datetime
    mag_total: int 
    mag_000_to_099: int
    mag_100_to_199: int
    mag_200_to_299: int
    mag_300_to_399: int
    mag_400_to_499: int
    mag_500_to_599: int
    mag_600_to_699: int
    mag_700_to_799: int
    mag_800_to_899: int
    mag_900_to_999: int


app_api = FastAPI(
    title="Amount Earthquake API",
    description="API สำหรับแสดงจำนวนแผ่นดินไหวล่าสุดจาก PostGIS",
    version="1.0.0"
)

# สร้างตัวแปรเชื่อต่อกับฐานข้อมูล Postgre
load_dotenv()
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

database_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = sqlalchemy.create_engine(database_url)

# กำหนดชื่อ ตาราง
TABLE_NAME = 'earthquakes'

# Query ข้อมูลที่ต้องการจะสร้าง API
text_query = sqlalchemy.text(f"""
    SELECT
        DATE_TRUNC('hour',TIMEZONE('Asia/Bangkok',TO_TIMESTAMP(time/1000))) AS hour_of_day,
        COUNT(CASE WHEN mag >= 0.00 AND mag < 10.00 THEN 1 ELSE NULL END) AS mag_total,
        COUNT(CASE WHEN mag >= 0.00 AND mag < 1.00 THEN 1 ELSE NULL END) AS mag_000_to_099,
        COUNT(CASE WHEN mag >= 1.00 AND mag < 2.00 THEN 1 ELSE NULL END) AS mag_100_to_199,
        COUNT(CASE WHEN mag >= 2.00 AND mag < 3.00 THEN 1 ELSE NULL END) AS mag_200_to_299,
        COUNT(CASE WHEN mag >= 3.00 AND mag < 4.00 THEN 1 ELSE NULL END) AS mag_300_to_399,
        COUNT(CASE WHEN mag >= 4.00 AND mag < 5.00 THEN 1 ELSE NULL END) AS mag_400_to_499,
        COUNT(CASE WHEN mag >= 5.00 AND mag < 6.00 THEN 1 ELSE NULL END) AS mag_500_to_599,
        COUNT(CASE WHEN mag >= 6.00 AND mag < 7.00 THEN 1 ELSE NULL END) AS mag_600_to_699,
        COUNT(CASE WHEN mag >= 7.00 AND mag < 8.00 THEN 1 ELSE NULL END) AS mag_700_to_799,
        COUNT(CASE WHEN mag >= 8.00 AND mag < 9.00 THEN 1 ELSE NULL END) AS mag_800_to_899,
        COUNT(CASE WHEN mag >= 9.00 AND mag < 10.00 THEN 1 ELSE NULL END) AS mag_900_to_999
    FROM earthquakes
    WHERE DATE_TRUNC('hour',TIMEZONE('Asia/Bangkok',TO_TIMESTAMP(time/1000))) >= DATE_TRUNC('day',TIMEZONE('Asia/Bangkok',NOW()))
    GROUP BY hour_of_day;
""")

@app_api.get(
    "/count_earthquake_per_hour", # URL ที่ผู้ใช้จะเรียกใช้
    response_model=List[Mag_earthquakes], # บอกว่า Output ต้องเป็นรายการ (List) ของข้อมูลแผ่นดินไหว
    summary="ดึงข้อมูลจำนวนแผ่นดินไหวล่าสุด",
    description="ดึงรายการแผ่นดินไหวล่าสุดตามจำนวนที่กำหนด จาก PostGIS",
)


def create_api_get_hourly_earthquake_summary():

    try:
        with engine.connect() as connection:

            execute_text_query = connection.execute(text_query)
            summary_data = execute_text_query.fetchall()

            rows = []
            for row in summary_data:

                # แปลงข้อมูลจาก DB เป็น Dictionary
                row_dict = row._asdict()
                rows.append(row_dict)

            # ส่ง List of Dictionaries กลับ (FastAPI จะแปลงเป็น JSON)
            return rows
    except Exception as e:
        # จัดการข้อผิดพลาดฐานข้อมูล
        raise HTTPException(status_code=500, detail=f"Database connection or query failed: {str(e)}")
