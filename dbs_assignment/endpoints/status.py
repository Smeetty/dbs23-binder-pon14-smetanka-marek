from fastapi import APIRouter

from dbs_assignment.config import settings
import psycopg2
import json

router = APIRouter()


@router.get("/v1/status")
async def connect():
    conn = psycopg2.connect(
        user=settings.DATABASE_USER,
        password=settings.DATABASE_PASSWORD,
        host=settings.DATABASE_HOST,
        port=settings.DATABASE_PORT,
        database=settings.DATABASE_NAME)

    curr = conn.cursor()
    curr.execute("SELECT version();")
    version = curr.fetchall()

    return {
        'version': version[0][0]
    }


@router.get("/v1/passengers/{id}/companions")
async def connect(id):
    conn = psycopg2.connect(
        user=settings.DATABASE_USER,
        password=settings.DATABASE_PASSWORD,
        host=settings.DATABASE_HOST,
        port=settings.DATABASE_PORT,
        database=settings.DATABASE_NAME)
    curr = conn.cursor()
    curr.execute("\
        SELECT COUNT(flights.flight_id) as day_count,\
                 t2.passenger_id as passanger_id,\
                 (json_build_object('id', t2.passenger_id, 'name', t2.passenger_name,\
       'flights_count', COUNT(flights.flight_id), 'flights', json_agg(flights.flight_id order by (flights.flight_id))))\
        FROM bookings.flights\
        JOIN bookings.ticket_flights t on flights.flight_id = t.flight_id\
        JOIN bookings.tickets t2 on t.ticket_no = t2.ticket_no\
        WHERE t2.passenger_id != %s\
        AND flights.flight_id IN\
        (SELECT f.flight_id  FROM bookings.tickets\
        LEFT JOIN bookings.ticket_flights tf on tickets.ticket_no = tf.ticket_no\
        LEFT JOIN bookings.flights f on tf.flight_id = f.flight_id\
        WHERE tickets.passenger_id = (%s))\
        GROUP BY t2.passenger_id, t2.passenger_name\
        ORDER BY day_count DESC, passanger_id", (id, id))

    data = curr.fetchall()
    result = []
    for json in data:
        result.append(json[2])

    return {
        'results': result
    }
@router.get("/v1/bookings/{id}")
async def connect(id):
    conn = psycopg2.connect(
        user=settings.DATABASE_USER,
        password=settings.DATABASE_PASSWORD,
        host=settings.DATABASE_HOST,
        port=settings.DATABASE_PORT,
        database=settings.DATABASE_NAME)

    curr = conn.cursor()
    curr.execute("\
         SELECT bookings.book_ref as id,\
       book_date         as book_date,\
       json_build_object(\
               'id', t.ticket_no,\
               'passenger_id', t.passenger_id,\
               'passenger_name', t.passenger_name,\
               'boarding_no', bp.boarding_no,\
               'flight_no', fl.flight_no,\
               'seat', bp.seat_no,\
               'aircraft_code', fl.aircraft_code,\
               'arrival_airport', fl.arrival_airport,\
               'departure_airport', fl.departure_airport,\
               'scheduled_arrival', fl.scheduled_arrival,\
               'scheduled_departure', fl.scheduled_departure\
           )\
 FROM bookings.bookings\
         LEFT JOIN bookings.tickets t ON bookings.book_ref = t.book_ref\
         LEFT JOIN bookings.boarding_passes bp ON t.ticket_no = bp.ticket_no\
         LEFT JOIN bookings.flights fl ON bp.flight_id = fl.flight_id\
 WHERE bookings.book_ref = %s\
 ORDER BY t.ticket_no, bp.boarding_no", (id, ))

    data = curr.fetchall()
    result = {"id": data[0][0], "book_date": data[0][1], "boarding_passes": []}

    for json in data:
        result['boarding_passes'].append(json[2])

    return {
        'result': result
    }

@router.get("/v1/flights/late-departure/{delay}")
async def connect(delay):
    conn = psycopg2.connect(
        user=settings.DATABASE_USER,
        password=settings.DATABASE_PASSWORD,
        host=settings.DATABASE_HOST,
        port=settings.DATABASE_PORT,
        database=settings.DATABASE_NAME)
    curr = conn.cursor()
    curr.execute("\
           SELECT EXTRACT(EPOCH FROM actual_departure - scheduled_departure)/60 as delay,\
            json_build_object(\
                'flight_id', flight_id,\
                'flight_no', flight_no,\
                'delay', ROUND(EXTRACT(EPOCH FROM actual_departure - scheduled_departure)/60, 0)\
            )\
            FROM bookings.flights\
            WHERE EXTRACT(EPOCH FROM actual_departure - scheduled_departure)/60 >= %s\
            ORDER BY delay DESC, flight_id", (delay,))

    data = curr.fetchall()
    result = []
    for json in data:
        result.append(json[1])

    return {
        'results': result
    }

@router.get("/v1/top-airlines")
async def connect(limit):
    conn = psycopg2.connect(
        user=settings.DATABASE_USER,
        password=settings.DATABASE_PASSWORD,
        host=settings.DATABASE_HOST,
        port=settings.DATABASE_PORT,
        database=settings.DATABASE_NAME)


    return {
        'results': "result"
    }

@router.get("/v1/departures")
async def connect(airport, day):
    conn = psycopg2.connect(
        user=settings.DATABASE_USER,
        password=settings.DATABASE_PASSWORD,
        host=settings.DATABASE_HOST,
        port=settings.DATABASE_PORT,
        database=settings.DATABASE_NAME)
    curr = conn.cursor()


    return {
        'results': "result"
    }

@router.get("/v1/airports/{airport}/destinations")
async def connect(airport):
    conn = psycopg2.connect(
        user=settings.DATABASE_USER,
        password=settings.DATABASE_PASSWORD,
        host=settings.DATABASE_HOST,
        port=settings.DATABASE_PORT,
        database=settings.DATABASE_NAME)



    return {
        'results': "result"
    }

@router.get("/v1/airlines/{flight_no}/load")
async def connect(flight_no):
    conn = psycopg2.connect(
        user=settings.DATABASE_USER,
        password=settings.DATABASE_PASSWORD,
        host=settings.DATABASE_HOST,
        port=settings.DATABASE_PORT,
        database=settings.DATABASE_NAME)
    curr = conn.cursor()


    return {
        'results': "result"
    }

@router.get("/v1/airlines/{flight_no}/load-week")
async def connect(flight_no):
    conn = psycopg2.connect(
        user=settings.DATABASE_USER,
        password=settings.DATABASE_PASSWORD,
        host=settings.DATABASE_HOST,
        port=settings.DATABASE_PORT,
        database=settings.DATABASE_NAME)
    curr = conn.cursor()



    return {
        'result': "result[0]"
    }

