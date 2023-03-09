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
           SELECT json_build_object(\
               'id', bookings.book_ref,\
               'book_date', book_date,\
               'boarding_passes', (SELECT json_agg(json_build_object(\
                'id', t.passenger_id,\
                'passenger_id', t.passenger_name,\
                'passenger_name', fl.flight_no,\
                'boarding_no',  bp.boarding_no,\
                'flight_no', fl.flight_no,\
                'seat', bp.seat_no,\
                'aircraft_code',  fl.aircraft_code,\
                'arrival_airport', fl.arrival_airport,\
                'departure_airport', fl.departure_airport,\
                'scheduled_arrival', fl.scheduled_arrival,\
                'scheduled_departure', fl.scheduled_departure\
                )\
            ))\
           )\
    FROM bookings.bookings\
            LEFT JOIN bookings.tickets t ON bookings.book_ref = t.book_ref\
            LEFT JOIN bookings.ticket_flights tf ON t.ticket_no = tf.ticket_no\
            LEFT JOIN bookings.boarding_passes bp ON tf.ticket_no = bp.ticket_no\
            LEFT JOIN bookings.flights fl ON bp.flight_id = fl.flight_id\
    WHERE bookings.book_ref = %s\
    GROUP BY bookings.book_ref", (id,))

    data = curr.fetchall()
    result = []
    for json in data:
        result.append(json[0])

    return {
        'results': result
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
           SELECT EXTRACT(MINUTES FROM (actual_departure  - scheduled_departure)) as delay,\
            json_build_object(\
                'flight_id', flight_id,\
                'flight_no', flight_no,\
                'delay', EXTRACT(MINUTES FROM (actual_departure  - scheduled_departure))\
            )\
            FROM bookings.flights\
            WHERE EXTRACT(MINUTES FROM (actual_departure  - scheduled_departure)) >= %s\
            ORDER BY delay DESC", (delay,))

    data = curr.fetchall()
    result = []
    for json in data:
        result.append(json[1])

    return {
        'results': result
    }

@router.get("/v1/top-airlines")
async def connect(limit):
   return limit
@router.get("/v1/departures")
async def connect(airport, day):
    conn = psycopg2.connect(
        user=settings.DATABASE_USER,
        password=settings.DATABASE_PASSWORD,
        host=settings.DATABASE_HOST,
        port=settings.DATABASE_PORT,
        database=settings.DATABASE_NAME)
    curr = conn.cursor()
    curr.execute("\
          SELECT EXTRACT(week FROM scheduled_departure),\
            json_build_object(\
               'flight_id', flight_id,\
               'flight_no', flight_no,\
               'scheduled_departure', scheduled_departure\
            )\
            FROM bookings.flights\
            WHERE departure_airport = %s\
            AND EXTRACT(week FROM scheduled_departure) = %s\
            GROUP BY flight_id\
            ORDER BY scheduled_departure", (airport, day, ))

    data = curr.fetchall()
    result = []
    for json in data:
        result.append(json[1])

    return {
        'results': result
    }

@router.get("/v1/airports/{airport}/destination")
async def connect(airport):
    conn = psycopg2.connect(
        user=settings.DATABASE_USER,
        password=settings.DATABASE_PASSWORD,
        host=settings.DATABASE_HOST,
        port=settings.DATABASE_PORT,
        database=settings.DATABASE_NAME)
    curr = conn.cursor()
    curr.execute("\
          SELECT airport_code\
        FROM bookings.airports_data\
        LEFT JOIN bookings.flights f on airports_data.airport_code = f.arrival_airport\
        WHERE f.departure_airport = %s\
        GROUP BY airport_code\
        ORDER BY airport_code", (airport, ))

    data = curr.fetchall()
    result = []
    for json in data:
        result.append(json[0])

    return {
        'results': result
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
    curr.execute("\
          SELECT\
            json_build_object(\
               'id', flights.flight_id,\
               'aircraft_capacity', COUNT(s.seat_no),\
               'load', COUNT(ticket_no),\
               'percentage', ROUND(100 - ((COUNT(seat_no)/100) - (COUNT(ticket_no)/100)) * 100, 1)\
            )\
        FROM bookings.flights\
        LEFT JOIN bookings.seats s ON flights.aircraft_code = s.aircraft_code\
        LEFT JOIN bookings.ticket_flights tf ON flights.flight_id = tf.flight_id\
        WHERE flights.flight_no = %s\
        GROUP BY flights.flight_id\
        ORDER BY flights.flight_id", (flight_no, ))

    data = curr.fetchall()
    result = []
    for json in data:
        result.append(json[0])

    return {
        'results': result
    }

@router.get("/v1/airlines/{flight_no}/load-week")
async def connect(flight_no):

    return {
        'results': "result"
    }

