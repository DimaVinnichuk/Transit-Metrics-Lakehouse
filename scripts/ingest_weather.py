import os
import requests
import boto3
import json
from datetime import datetime
from dotenv import load_dotenv

# Load credentials
load_dotenv()

# Tønsberg, Vestfold coordinates
LAT = 59.26
LON = 10.40
URL = f"https://api.open-meteo.com/v1/forecast?latitude={LAT}&longitude={LON}&current_weather=true"

def ingest_weather():
    try:
        response = requests.get(URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Partitioning logic for Bronze layer
        now = datetime.now()
        partition_path = now.strftime("year=%Y/month=%m/day=%d")
        file_name = f"weather_{now.strftime('%H%M')}.json"
        s3_key = f"weather/{partition_path}/{file_name}"

        s3 = boto3.client(
            "s3",
            endpoint_url="http://localhost:9000",
            aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
            aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD")
        )

        s3.put_object(
            Bucket="bronze",
            Key=s3_key,
            Body=json.dumps(data),
            ContentType="application/json"
        )
        print(f"Weather data for VOT saved to {s3_key}")
    except Exception as e:
        print(f"Weather ingestion failed: {e}")

if __name__ == "__main__":
    ingest_weather()