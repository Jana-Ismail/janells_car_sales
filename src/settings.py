import os
from dotenv import load_dotenv

load_dotenv()

API_USERNAME = os.getenv("JANELL_API_USERNAME")
API_PASSWORD = os.getenv("JANELL_API_PASSWORD")
API_BASE_URL = os.getenv("JANELL_API_BASE_URL")

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
