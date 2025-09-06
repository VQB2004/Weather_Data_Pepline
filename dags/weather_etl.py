import requests
from minio import Minio
import os
from datetime import datetime
import os
from dotenv import load_dotenv
from io import BytesIO
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


API_KEY="ae51175e7f8c7a3a6caca639a9175b5f"
city_name="Ho Chi Minh"

client = Minio(
#"localhost:9000",
"minio:9000",   # endpoint MinIO
access_key="admin",
secret_key="minio123",
secure=False       
)
bucket_name = "weather"


def load_to_minio():
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={API_KEY}&units=metric&lang=en"
    try:
        response = requests.get(url)
        data = response.json()

        # tạo dict dữ liệu thời tiết
        weather_data = {
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "wind_speed": data["wind"]["speed"],
            "weather": data["weather"][0]["main"],
            "description": data["weather"][0]["description"],
            "call_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        # upload trực tiếp lên MinIO
        object_name = f"weather_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df = pd.DataFrame([weather_data])
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False, encoding="utf-8")
        csv_buffer.seek(0)

        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)

        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=csv_buffer,
            length=csv_buffer.getbuffer().nbytes,
            content_type="text/csv"
        )
        print(f"Uploaded to MinIO: {bucket_name}/{object_name}")

    except requests.exceptions.RequestException as e:
        print(f"API CALL FAILED: {e}")
    except ValueError as ve:
        print(f"Data error: {ve}")
    except Exception as ex:
        print(f"Unexpected error: {ex}")

def load_from_minio_to_postgre():
    engine = create_engine("postgresql+psycopg2://admin:admin123@postgres_db:5432/etl")
    try:
        loaded_files = pd.read_sql("SELECT file_name FROM metadata",con=engine)
        loaded_files_set = set(loaded_files["file_name"].tolist())
    except Exception as e:
        loaded_files_set = set()
        print(e)
        return
    objects = client.list_objects(bucket_name, recursive=True)

    for obj in objects:
        file_name = obj.object_name

        # Nếu file đã tồn tại trong metadata thì bỏ qua
        if file_name in loaded_files_set:
            continue

        # Nếu file mới thì tải về
        response = client.get_object(bucket_name, file_name)
        df = pd.read_csv(BytesIO(response.read()))

        # Load vào PostgreSQL
        df.to_sql("weather_hcm", engine, if_exists="append", index=False)

        meta_df = pd.DataFrame([{"file_name": file_name}])
        meta_df.to_sql("metadata", engine, if_exists="append", index=False)

        print(f"Loaded {file_name} → PostgreSQL")
