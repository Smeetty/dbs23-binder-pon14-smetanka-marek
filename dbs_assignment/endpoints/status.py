from fastapi import APIRouter

from dbs_assignment.config import settings
import psycopg2

router = APIRouter()
conn = psycopg2.connect(
    user=settings.DATABASE_USER,
    password=settings.DATABASE_PASSWORD,
    host=settings.DATABASE_HOST,
    port=settings.DATABASE_PORT,
    database=settings.DATABASE_NAME)


@router.get("/v1/status")
async def connect():
    curr = conn.cursor()
    curr.execute("SELECT version();")
    version = curr.fetchall()

    return {
        'version': version[0][0]
    }


@router.get("/v1/passengers/{id}/companions")
async def connect(id):
    curr = conn.cursor()
    curr.execute("\
        SELECT json_build_object('id', t2.passenger_id, 'name', t2.passenger_name,\
       'flights_count', COUNT(flights.flight_id), 'flights', json_agg(flights.flight_id))\
        FROM flights\
        JOIN ticket_flights t on flights.flight_id = t.flight_id\
        JOIN tickets t2 on t.ticket_no = t2.ticket_no\
        WHERE\
        flights.flight_id IN\
        (SELECT f.flight_id  FROM tickets\
        LEFT JOIN ticket_flights tf on tickets.ticket_no = tf.ticket_no\
        LEFT JOIN flights f on tf.flight_id = f.flight_id\
        WHERE tickets.passenger_id = (%s))\
        GROUP BY t2.passenger_id, t2.passenger_name", (id,))

    result = curr.fetchall()
    data = []
    for r in result:
        data.append(r)

    return {
        'results': result
    }
