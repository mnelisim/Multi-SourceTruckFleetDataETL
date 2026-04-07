# config.py
from dotenv import load_dotenv
import os

load_dotenv()  # loads variables from .env

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")


# ===================== CONFIG DATABASE ====================
# user = os.getenv("user")
# password = os.getenv("password")
# db_name = os.getenv("db_name")

# db_url = os.getenv("db_url")
db_url = os.getenv("POSTGRES_URL")
user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
db_name = os.getenv("POSTGRES_DB")
dash_url = os.getenv("DASH_URL")


# You may need to construct this if not provided
#db_url = f"jdbc:postgresql://postgres_db:5432/{db_name}"