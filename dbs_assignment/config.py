from pydantic import BaseSettings
import os
from dotenv import load_dotenv
load_dotenv()
class Settings(BaseSettings):

    DATABASE_USER: str = os.getenv("DATABASE_USER", "postgres")
    DATABASE_PASSWORD: str = os.getenv("DATABASE_PASSWORD", "tajneheslo")
    DATABASE_HOST: str = os.getenv("DATABASE_HOST", "localhost")
    DATABASE_PORT: str = os.getenv("DATABASE_PORT", 5432)  # default postgres port is 5432
    DATABASE_NAME: str = os.getenv("DATABASE_NAME", "postgres")
    DATABASE_URL = f"postgresql://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}"

settings = Settings()
