import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # MinIO settings
    MINIO_USER = os.getenv('MINIO_ROOT_USER')
    MINIO_PASS = os.getenv('MINIO_ROOT_PASSWORD')
    MINIO_ENDPOINT = "localhost:9000"
    
    # Postgres settings
    DB_USER = os.getenv('POSTGRES_USER')
    DB_PASS = os.getenv('POSTGRES_PASSWORD')
    DB_NAME = os.getenv('POSTGRES_DB')
    DB_HOST = "localhost"
    DB_PORT = "5432"