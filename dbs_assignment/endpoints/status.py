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
    curr = conn.cursor()
    curr.execute("\
         SELECT count(DISTINCT t.ticket_no) as count, json_build_object(\
    'flight_no', flights.flight_no,\
    'count', count(t.ticket_no))\
        FROM bookings.flights\
 LEFT JOIN bookings.ticket_flights tf on flights.flight_id = tf.flight_id\
 LEFT JOIN bookings.tickets t on tf.ticket_no = t.ticket_no\
        GROUP BY flights.flight_no\
 ORDER BY count DESC, flight_no DESC LIMIT %s", (limit,))

    data = curr.fetchall()
    result = []
    for json in data:
        result.append(json[1])

    return {
        'results': result
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
    curr.execute("\
          SELECT json_build_object(\
               'flight_id', flight_id,\
               'flight_no', flight_no,\
               'scheduled_departure', scheduled_departure\
            )\
            FROM bookings.flights\
            WHERE departure_airport = %s\
            AND EXTRACT(ISODOW FROM scheduled_departure) = %s\
            AND flights.status = 'Scheduled'\
            GROUP BY flight_id\
            ORDER BY scheduled_departure, flight_id", (airport, day, ))

    data = curr.fetchall()
    result = []
    for json in data:
        result.append(json[0])

    return {
        'results': result
    }

@router.get("/v1/airports/{airport}/destinations")
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
    curr.execute("SELECT \
                 json_build_object( 'id', flights.flight_id, 'aircraft_capacity',\
                 (SELECT COUNT(seats.seat_no) FROM seats WHERE aircraft_code = flights.aircraft_code ),\
                 'load', COUNT(tf.ticket_no), 'percentage_load', ROUND((cast(COUNT(tf.ticket_no) as decimal) / cast(( SELECT COUNT(seats.seat_no) FROM bookings.seats WHERE aircraft_code = flights.aircraft_code ) as decimal) * 100), 2)) FROM bookings.flights\
                 LEFT JOIN bookings.ticket_flights tf on flights.flight_id = tf.flight_id\
                 WHERE flights.flight_no = %s GROUP BY flights.flight_id ORDER BY flights.flight_id", (flight_no, ))

    data = curr.fetchall()
    result = []
    for json in data:
        result.append(json[0])

    return {
        'results': result
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
    curr.execute("SELECT\
 json_build_object(\
        'flight_no', flights.flight_no,\
        'monday', (SELECT\
        ROUND((SELECT cast(COUNT(tf.ticket_no) as decimal)\
                         FROM bookings.flights\
                         LEFT JOIN bookings.ticket_flights tf on flights.flight_id = tf.flight_id\
                         WHERE flights.flight_no = (%s) AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 1 GROUP BY flights.aircraft_code)\
                         / cast(COUNT(s.seat_no) as decimal) * 100, 2)\
         FROM bookings.flights\
         LEFT JOIN bookings.seats s on flights.aircraft_code = s.aircraft_code\
         WHERE flights.flight_no = (%s) AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 1 GROUP BY flights.aircraft_code)\
        ,\
        'tuesday', (SELECT\
        ROUND((SELECT cast(COUNT(tf.ticket_no) as decimal)\
                         FROM bookings.flights\
                         LEFT JOIN bookings.ticket_flights tf on flights.flight_id = tf.flight_id\
                         WHERE flights.flight_no = (%s) AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 2 GROUP BY flights.aircraft_code)\
                         / cast(COUNT(s.seat_no) as decimal) * 100, 2)\
         FROM bookings.flights\
         LEFT JOIN bookings.seats s on flights.aircraft_code = s.aircraft_code\
         WHERE flights.flight_no = (%s) AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 2 GROUP BY flights.aircraft_code)\
        ,\
        'wendesday', (SELECT\
        ROUND((SELECT cast(COUNT(tf.ticket_no) as decimal)\
                         FROM bookings.flights\
                         LEFT JOIN bookings.ticket_flights tf on flights.flight_id = tf.flight_id\
                         WHERE flights.flight_no = (%s) AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 3 GROUP BY flights.aircraft_code)\
                         / cast(COUNT(s.seat_no) as decimal) * 100, 2)\
         FROM bookings.flights\
         LEFT JOIN bookings.seats s on flights.aircraft_code = s.aircraft_code\
         WHERE flights.flight_no = (%s) AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 3 GROUP BY flights.aircraft_code)\
        ,\
        'thursday', (SELECT\
        ROUND((SELECT cast(COUNT(tf.ticket_no) as decimal)\
                         FROM bookings.flights\
                         LEFT JOIN bookings.ticket_flights tf on flights.flight_id = tf.flight_id\
                         WHERE flights.flight_no = (%s) AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 4 GROUP BY flights.aircraft_code)\
                         / cast(COUNT(s.seat_no) as decimal) * 100, 2)\
         FROM bookings.flights\
         LEFT JOIN bookings.seats s on flights.aircraft_code = s.aircraft_code\
         WHERE flights.flight_no = (%s) AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 4 GROUP BY flights.aircraft_code)\
        ,\
        'friday', (SELECT\
        ROUND((SELECT cast(COUNT(tf.ticket_no) as decimal)\
                         FROM bookings.flights\
                         LEFT JOIN bookings.ticket_flights tf on flights.flight_id = tf.flight_id\
                         WHERE flights.flight_no = (%s) AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 5 GROUP BY flights.aircraft_code)\
                         / cast(COUNT(s.seat_no) as decimal) * 100, 2)\
         FROM bookings.flights\
         LEFT JOIN bookings.seats s on flights.aircraft_code = s.aircraft_code\
         WHERE flights.flight_no = (%s) AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 5 GROUP BY flights.aircraft_code)\
        ,\
        'saturday', (SELECT\
        ROUND((SELECT cast(COUNT(tf.ticket_no) as decimal)\
                         FROM bookings.flights\
                         LEFT JOIN bookings.ticket_flights tf on flights.flight_id = tf.flight_id\
                         WHERE flights.flight_no = (%s) AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 6 GROUP BY flights.aircraft_code)\
                         / cast(COUNT(s.seat_no) as decimal) * 100, 2)\
         FROM bookings.flights\
         LEFT JOIN bookings.seats s on flights.aircraft_code = s.aircraft_code\
         WHERE flights.flight_no = (%s) AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 6 GROUP BY flights.aircraft_code)\
        ,\
        'sunday', (SELECT\
        ROUND((SELECT cast(COUNT(tf.ticket_no) as decimal)\
                         FROM bookings.flights\
                         LEFT JOIN bookings.ticket_flights tf on flights.flight_id = tf.flight_id\
                         WHERE flights.flight_no = (%s) AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 7 GROUP BY flights.aircraft_code)\
                         / cast(COUNT(s.seat_no) as decimal) * 100, 2)\
         FROM bookings.flights\
         LEFT JOIN bookings.seats s on flights.aircraft_code = s.aircraft_code\
         WHERE flights.flight_no = (%s) AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 7 GROUP BY flights.aircraft_code)\
) FROM bookings.flights\
 WHERE flights.flight_no = (%s) GROUP BY flight_no", (flight_no, flight_no, flight_no, flight_no, flight_no, flight_no, flight_no, flight_no, flight_no, flight_no, flight_no, flight_no, flight_no, flight_no, flight_no, ))

    data = curr.fetchall()
    result = []
    for json in data:
        result.append(json[0])


    return {
        'result': result[0]
    }

# ------------------------------------ ZADANIE 2 ------------------------------------ #
@router.get("/v3/air-time/{book_ref}")
async def connect(book_ref):
    conn = psycopg2.connect(
        user=settings.DATABASE_USER,
        password=settings.DATABASE_PASSWORD,
        host=settings.DATABASE_HOST,
        port=settings.DATABASE_PORT,
        database=settings.DATABASE_NAME)

    curr = conn.cursor()
    curr.execute("SELECT tickets.ticket_no, tickets.passenger_name,\
       array(\
           SELECT\
           (flights.departure_airport,\
           flights.arrival_airport,\
           flights.flight_no,\
           TO_CHAR((sum(EXTRACT(EPOCH FROM (flights.actual_arrival - flights.actual_departure)))\
               OVER (ORDER BY flights.flight_id) || ' second')::interval, 'HH24:MI:SS'),\
           TO_CHAR((EXTRACT(EPOCH FROM (flights.actual_arrival - flights.actual_departure)) || ' second')::interval, 'HH24:MI:SS')\
           ) FROM flights\
           LEFT JOIN ticket_flights tf on flights.flight_id = tf.flight_id\
           LEFT JOIN tickets t on tf.ticket_no = t.ticket_no\
           WHERE t.ticket_no = tickets.ticket_no\
           AND t.passenger_id = tickets.passenger_id\
           ORDER BY flights.actual_departure\
           )\
    FROM tickets\
    WHERE tickets.book_ref = %s\
    GROUP BY tickets.ticket_no\
    ORDER BY tickets.ticket_no", (book_ref,))

    data = curr.fetchall()
    result = {"id": data[0][0], "book_date": data[0][1], "boarding_passes": []}

    for json in data:
        result['boarding_passes'].append(json[2])

    return {
        'result': result
    }
