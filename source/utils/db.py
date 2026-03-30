import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
WATERMARK_SECRET_KEY = os.getenv("WATERMARK_SECRET_KEY")
engine = create_engine(DATABASE_URL)