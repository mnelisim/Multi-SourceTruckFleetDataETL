# config.py
from dotenv import load_dotenv
import os

load_dotenv()  # loads variables from .env

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")


# ===================== CONFIG DATABASE ====================




db_url = os.getenv("POSTGRES_URL")
user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
db_name = os.getenv("POSTGRES_DB")
dash_url = os.getenv("DASH_URL")


