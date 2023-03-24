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
 ORDER BY t.ticket_no, bp.boarding_no", (id,))

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
            ORDER BY scheduled_departure, flight_id", (airport, day,))

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
        ORDER BY airport_code", (airport,))

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
                 WHERE flights.flight_no = %s GROUP BY flights.flight_id ORDER BY flights.flight_id", (flight_no,))

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
 WHERE flights.flight_no = (%s) GROUP BY flight_no", (
        flight_no, flight_no, flight_no, flight_no, flight_no, flight_no, flight_no, flight_no, flight_no, flight_no,
        flight_no, flight_no, flight_no, flight_no, flight_no,))

    data = curr.fetchall()
    result = []
    for json in data:
        result.append(json[0])

    return {
        'result': result[0]
    }


# ------------------------------------ ZADANIE 2 ------------------------------------ #
@router.get("/v3/aircrafts/{aircraft_code}/seats/{seat_choice}")
async def connect(aircraft_code, seat_choice):
    conn = psycopg2.connect(
        user=settings.DATABASE_USER,
        password=settings.DATABASE_PASSWORD,
        host=settings.DATABASE_HOST,
        port=settings.DATABASE_PORT,
        database=settings.DATABASE_NAME)

    curr = conn.cursor()
    curr.execute("SELECT seat_no,\
       count(*) as count\
    FROM (SELECT bp.seat_no                                                    as seat_no,\
             dense_rank() over (PARTITION BY bp.flight_id order by b.book_date) as rank\
      from bookings.boarding_passes bp\
            JOIN bookings.tickets t ON bp.ticket_no = t.ticket_no\
            JOIN bookings.bookings b ON t.book_ref = b.book_ref\
            WHERE bp.flight_id IN (\
                SELECT flight_id\
                FROM bookings.flights\
                WHERE aircraft_code LIKE %s\
            )\
      ) as what\
    WHERE RANK = %s\
    group by seat_no\
    order by count desc\
    limit 1", (aircraft_code, seat_choice))

    data = curr.fetchall()
    result = {"seat": data[0][0], "count": data[0][1]}

    return {
        'result': result
    }


@router.get("/v3/air-time/{book_ref}")
async def connect(book_ref):
    conn = psycopg2.connect(
        user=settings.DATABASE_USER,
        password=settings.DATABASE_PASSWORD,
        host=settings.DATABASE_HOST,
        port=settings.DATABASE_PORT,
        database=settings.DATABASE_NAME)

    curr = conn.cursor()
    curr.execute("SELECT tickets.ticket_no,\
                 tickets.passenger_name,\
       array(\
           SELECT\
           (flights.departure_airport,\
           flights.arrival_airport,\
           flights.flight_no,\
           TO_CHAR((sum(EXTRACT(EPOCH FROM (flights.actual_arrival - flights.actual_departure)))\
               OVER (ORDER BY flights.flight_id) || ' second')::interval, 'HH24:MI:SS'),\
           TO_CHAR((EXTRACT(EPOCH FROM (flights.actual_arrival - flights.actual_departure)) || ' second')::interval, 'HH24:MI:SS')\
           ) as total_f_time FROM bookings.flights\
           LEFT JOIN bookings.ticket_flights tf on flights.flight_id = tf.flight_id\
           LEFT JOIN bookings.tickets t on tf.ticket_no = t.ticket_no\
           WHERE t.ticket_no = tickets.ticket_no\
           AND t.passenger_id = tickets.passenger_id\
           ORDER BY total_f_time DESC\
           )\
    FROM bookings.tickets\
    WHERE tickets.book_ref = %s\
    GROUP BY tickets.ticket_no\
    ORDER BY tickets.ticket_no", (book_ref,))

    data = curr.fetchall()
    result = []
    for json in data:
        innerData = []

        s = json[2]
        s = s.replace('{', '').replace('}', '')  # Remove curly braces
        lst = [tuple(i.strip('()').split(',')) for i in s.split('","')]  # Convert string into list of tuples
        for format in lst:
            innerData.append({
                    'departure_airport': format[0].replace('"', '').replace('(', '').replace(')', ''),
                    'arrival_airport': format[1].replace('"', '').replace('(', '').replace(')', ''),
                    'flight_time': format[4].replace('"', '').replace('(', '').replace(')', ''),
                    'total_time': format[3].replace('"', '').replace('(', '').replace(')', ''),
                })

        result.append({"ticket_no": json[0], "passenger_name": json[1], "flights": innerData})

    return {
        'result': result
    }

@router.get("/v3/airlines/{flight_no}/top_seats")
async def connect(flight_no, limit):
    conn = psycopg2.connect(
        user=settings.DATABASE_USER,
        password=settings.DATABASE_PASSWORD,
        host=settings.DATABASE_HOST,
        port=settings.DATABASE_PORT,
        database=settings.DATABASE_NAME)

    curr = conn.cursor()
    curr.execute("SELECT seat_no as seat, flight_count, (ARRAY_AGG(flight_id ORDER BY flight_id))[0:5] as flights\
    FROM (SELECT flight_id,\
             seat_no,\
             COUNT(sub1.rank) OVER (PARTITION BY rank) as flight_count\
      FROM (SELECT boarding_passes.seat_no,\
                   f.flight_id,\
                   f.flight_id - DENSE_RANK() OVER (ORDER BY boarding_passes.seat_no, f.flight_id) as rank\
            FROM bookings.flights f\
                     JOIN bookings.boarding_passes ON f.flight_id = boarding_passes.flight_id\
            WHERE f.flight_id IN (SELECT flight_id\
                                  FROM bookings.flights\
                                  WHERE flight_no LIKE %s)) as sub1\
      ORDER BY sub1.seat_no\
      ) as sub\
    GROUP BY seat, flight_count\
    ORDER BY flight_count DESC LIMIT %s", (flight_no, limit))

    data = curr.fetchall()
    result = []
    for json in data:
        result.append({"seat": json[0], "flight_count": json[1], "fligths": json[2]})

    return {
        'result': result
    }


@router.get("/v3/aircrafts/{aircraft_code}/top-incomes")
async def connect(aircraft_code):
    conn = psycopg2.connect(
        user=settings.DATABASE_USER,
        password=settings.DATABASE_PASSWORD,
        host=settings.DATABASE_HOST,
        port=settings.DATABASE_PORT,
        database=settings.DATABASE_NAME)

    curr = conn.cursor()
    curr.execute("SELECT total_amount, month, day, rank\
    FROM (SELECT sum(amount)                                                                                          as total_amount,\
             CONCAT(DATE_PART('year', f.scheduled_departure), '-',\
                    DATE_PART('month', f.scheduled_departure))                                                    as month,\
             DATE_PART('day', f.scheduled_departure)                                                              as day,\
             RANK() OVER (PARTITION BY CONCAT(DATE_PART('year', scheduled_departure), '-',\
                                              DATE_PART('month', scheduled_departure)) order by sum(amount) desc) as rank\
      FROM bookings.ticket_flights\
               LEFT JOIN bookings.flights f on ticket_flights.flight_id = f.flight_id\
      WHERE f.aircraft_code = %s\
        AND actual_departure is not null\
      GROUP BY DATE_PART('day', f.scheduled_departure), DATE_PART('month', f.scheduled_departure),\
               DATE_PART('year', f.scheduled_departure),\
               DATE_TRUNC('month', f.scheduled_departure)) as was\
    WHERE rank = 1\
    ORDER BY total_amount DESC, month", (aircraft_code,))

    data = curr.fetchall()
    result = []
    for json in data:
        result.append({"total_amount": json[0], "month": json[1], "day": json[2]})

    return {
        'result': result
    }
