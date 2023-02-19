from fastapi import APIRouter

from dbs_assignment.config import settings
import psycopg2

router = APIRouter()


@router.get("/v1/status")
async def connect():
    conn = psycopg2.connect(
        user=settings.DATABASE_USER,
        password=settings.DATABASE_PASSWORD,
        host=settings.DATABASE_HOST,
        port=settings.DATABASE_PORT,
        database=settings.DATABASE_NAME)

    if conn:
        curr = conn.cursor()
        curr.execute("SELECT version();")
        version = curr.fetchall()
        return {
            'version': version[0][0]
        }
