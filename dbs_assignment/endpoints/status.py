from fastapi import APIRouter

from dbs_assignment.config import settings
import psycopg2

router = APIRouter()


@router.get("/v1/status")
async def status():
    connection = psycopg2.connect(
        dbname=settings.DATABASE_NAME,
        user=settings.DATABASE_USER,
        password=settings.DATABASE_PASSWORD,
        host=settings.DATABASE_HOST,
        port=settings.DATABASE_PORT,
    )
    if connection:
        cursor = connection.cursor()
        return {
            'version': cursor.execute("SELECT version();")
        }
