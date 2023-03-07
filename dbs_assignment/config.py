from pydantic import BaseSettings
import os
from dotenv import load_dotenv
load_dotenv()
class Settings(BaseSettings):

    DATABASE_USER: str = os.getenv("DATABASE_USER", "postgres")
    DATABASE_PASSWORD: str = os.getenv("DATABASE_PASSWORD", "postgres")
    DATABASE_HOST: str = os.getenv("DATABASE_HOST", "127.0.0.1")
    DATABASE_PORT: str = os.getenv("DATABASE_PORT", 5432)  # default postgres port is 5432
    DATABASE_NAME: str = os.getenv("DATABASE_NAME", "postgres")

settings = Settings()
