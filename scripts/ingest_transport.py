import os
import requests
import boto3
import json
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

URL = "https://api.entur.io/realtime/v2/vehicles/graphql"
HEADERS = {
    'ET-Client-Name': f'transit-metrics-lakehouse-{os.getenv("CONTACT_EMAIL")}',
    'Content-Type': 'application/json'
}

# Cleanest possible query to avoid "Undefined Field" errors
QUERY = """
{
  vehicles(codespaceId: "VOT") {
    lastUpdated
    location {
      latitude
      longitude
    }
    line {
      publicCode
    }
    delay
  }
}
"""

def ingest_transport():
    try:
        print("Requesting data for Vestfold (VOT)...")
        response = requests.post(URL, json={'query': QUERY}, headers=HEADERS, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if 'errors' in data:
            print(f"GraphQL Error: {data['errors']}")
            return

        vehicles = data.get('data', {}).get('vehicles', [])
        
        now = datetime.now()
        partition_path = now.strftime("year=%Y/month=%m/day=%d")
        file_name = f"vot_transport_{now.strftime('%H%M')}.json"
        s3_key = f"transport/{partition_path}/{file_name}"

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
        print(f"Successfully uploaded {len(vehicles)} vehicles to {s3_key}")

    except Exception as e:
        print(f"Ingestion failed: {e}")

if __name__ == "__main__":
    ingest_transport()