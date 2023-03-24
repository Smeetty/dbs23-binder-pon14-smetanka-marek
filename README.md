# Zadanie 3

## Endpoint - 1 (2B)
Požiadavka: GET /v3/aircrafts/:aircraft_code/seats/:seat_choice <br>
Popis: <br>

Pomocou window funckie dense rank PARTITION BY bp.flight_id order by b.book_date získame všetky boarding_passes
, ktoré po LEFT JOINe majú flight id, také, ktoré obsahuje aircraft_code zadané ako pamareter. Táto funkcia nám
zabezpečí, že získame ohodnotenie podľa book date, s tým že rôzne hodnoty budu pokračovať n+1 kde skončili.
takže čísla budú stale 1,2,3 nie 1,5 atď. podľa toho vyberieme tie, ktoré potrebujeme najčastejšie (WHERE rank 1).

Python:

```shell
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
```
PostgreSql:

```shell
SELECT seat_no,
       count(*) as count
FROM (SELECT bp.seat_no                                                    as seat_no,
             dense_rank() over (PARTITION BY bp.flight_id order by b.book_date) as rank
      from boarding_passes bp
            JOIN bookings.tickets t ON bp.ticket_no = t.ticket_no
            JOIN bookings.bookings b ON t.book_ref = b.book_ref
            WHERE bp.flight_id IN (
                SELECT flight_id
                FROM bookings.flights
                WHERE aircraft_code LIKE 'SU9'
            )
      ) as suck
where rank = 2
group by seat_no
order by count desc
limit 1
```

## Endpoint - 2 (2B)
Požiadavka: GET /v3/air-time/:book_ref <br>
Popis: <br>
Získame všetky lety, ktoré majú ticker.book_ref poďla parametrov. V subselecte získame rozdiel dátumov a pomocou
window funkcie sum over order podľa scheduled_departure získame súčet tohto rozdielu podľa scheduled_departure.

Python:

```shell
  curr.execute("SELECT tickets.ticket_no,\
                 tickets.passenger_name,\
       array(\
           SELECT\
           (flights.departure_airport,\
           flights.arrival_airport,\
           TO_CHAR((sum(EXTRACT(EPOCH FROM (flights.actual_arrival - flights.actual_departure)))\
               OVER (ORDER BY flights.scheduled_departure) || ' second')::interval, 'HH24:MI:SS') ,\
           TO_CHAR((EXTRACT(EPOCH FROM (flights.actual_arrival - flights.actual_departure)) || ' second')::interval, 'HH24:MI:SS')\
           ) as total_f_time FROM bookings.flights\
           LEFT JOIN bookings.ticket_flights tf on flights.flight_id = tf.flight_id\
           LEFT JOIN bookings.tickets t on tf.ticket_no = t.ticket_no\
               WHERE t.ticket_no = tickets.ticket_no\
           AND t.passenger_id = tickets.passenger_id\
            ORDER BY flights.scheduled_departure\
           )\
    FROM bookings.tickets\
    WHERE tickets.book_ref = %s\
    GROUP BY tickets.ticket_no\
    ORDER BY tickets.ticket_no", (book_ref,))
```

PostgreSql:

```shell
SELECT tickets.ticket_no, tickets.passenger_name,
       array(
           SELECT
           (flights.departure_airport,
           flights.arrival_airport,
           flights.flight_no,
           TO_CHAR((sum(EXTRACT(EPOCH FROM (flights.actual_arrival - flights.actual_departure)))
               OVER (ORDER BY flights.flight_id) || ' second')::interval, 'HH24:MI:SS'),
           TO_CHAR((EXTRACT(EPOCH FROM (flights.actual_arrival - flights.actual_departure)) || ' second')::interval, 'HH24:MI:SS')
           ) as  total_f_time FROM flights
           LEFT JOIN ticket_flights tf on flights.flight_id = tf.flight_id
           LEFT JOIN tickets t on tf.ticket_no = t.ticket_no
           WHERE t.ticket_no = tickets.ticket_no
           AND t.passenger_id = tickets.passenger_id
           ORDER BY total_f_time DESC
           )
FROM tickets
WHERE tickets.book_ref = '8D344B'
GROUP BY tickets.ticket_no
ORDER BY tickets.ticket_no
```


## Endpoint - 3 (2B)
Požiadavka: GET /v3/airlines/:flight_no/top_seats?limit=:limit <br>
Popis: <br>
Získame flight_ids, kde flight_no sa rovná parametru. použijeme window funkciu dense rank, pomocu ktorej získame
počet flight_count. To limitujeme pomocou limitu, ktorý získame ako parameter.

Python:

```shell
    curr.execute("SELECT seat_no as seat, flight_count, ARRAY_AGG(flight_id ORDER BY flight_id) as flights\
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
                                  WHERE flight_no = %s)) as sub1\
      ORDER BY sub1.seat_no\
      ) as sub\
    GROUP BY seat, flight_count\
    ORDER BY flight_count DESC LIMIT %s", (flight_no, limit))
```

PostgreSql:

```shell
SELECT seat_no as seat, flight_count, ARRAY_AGG(flight_id ORDER BY flight_id) as flights
FROM (SELECT flight_id,
             seat_no,
             COUNT(sub1.rank) OVER (PARTITION BY rank) as flight_count
      FROM (SELECT boarding_passes.seat_no,
                   f.flight_id,
                   f.flight_id - DENSE_RANK() OVER (ORDER BY boarding_passes.seat_no, f.flight_id) as rank
            FROM bookings.flights f
                     JOIN bookings.boarding_passes ON f.flight_id = boarding_passes.flight_id
            WHERE f.flight_id IN (SELECT flight_id
                                  FROM bookings.flights
                                  WHERE flight_no = 'PG0019')) as sub1
      ORDER BY sub1.seat_no
      ) as sub
GROUP BY seat, flight_count
ORDER BY flight_count DESC
LIMIT 1
```

## Endpoint - 4 (2B)
Požiadavka: GET /v3/aircrafts/:aircraft_code/top-incomes <br>
Popis: <br>
Získame pomocou funkcie DATE_PART časti z timestampu napríklad mesiac, rok a deň. Pomocou funkcie concat
spojíme dva stringy napríklad dátum. Spojíme iba tie dátumy, ktoré majú rovnaké yyyy-mm dátum a zoradíme podľa
sum(amount (tú su zaradené všekty tickety daného letu)). Takto ohodnotíme skupiny (yyyy-mm) a ohodnotíme podĺa sum(amount (tú su zaradené všekty tickety daného letu)).


Python:

```shell
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
```

PostgreSql:

```shell
SELECT total_amount, month, day, rank
    FROM
(SELECT sum(amount) as total_amount, CONCAT(DATE_PART('year', f.scheduled_departure), '-', DATE_PART('month', f.scheduled_departure)) as month,
       DATE_PART('day', f.scheduled_departure) as day,
       RANK() OVER(PARTITION BY CONCAT(DATE_PART('year', scheduled_departure), '-', DATE_PART('month', scheduled_departure)) order by sum(amount) desc) as rank
       FROM ticket_flights
LEFT JOIN flights f on ticket_flights.flight_id = f.flight_id
WHERE f.aircraft_code = '763'
AND actual_departure is not null
GROUP BY DATE_PART('day', f.scheduled_departure), DATE_PART('month', f.scheduled_departure), DATE_PART('year', f.scheduled_departure),
         DATE_TRUNC('month', f.scheduled_departure)
) as was
    where rank = 1
ORDER BY total_amount DESC, month
```




# Zadanie 2 Dokumentácia k endpointom

## Endpoint - zoznam spolucestujúcich (1b)
Požiadavka: GET /v1/passengers/:passenger_id/companions <br>
Python:

```shell
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
```

SQL:
```shell
 SELECT COUNT(flights.flight_id) as day_count,
                 t2.passenger_id as passanger_id,
                 (json_build_object('id', t2.passenger_id, 'name', t2.passenger_name,
       'flights_count', COUNT(flights.flight_id), 'flights', json_agg(flights.flight_id order by (flights.flight_id))))
        FROM bookings.flights
        JOIN bookings.ticket_flights t on flights.flight_id = t.flight_id
        JOIN bookings.tickets t2 on t.ticket_no = t2.ticket_no
        WHERE t2.passenger_id != '5260 0236351'
        AND flights.flight_id IN
        (SELECT f.flight_id  FROM bookings.tickets
        LEFT JOIN bookings.ticket_flights tf on tickets.ticket_no = tf.ticket_no
        LEFT JOIN bookings.flights f on tf.flight_id = f.flight_id
        WHERE tickets.passenger_id = '5260 236351')
        GROUP BY t2.passenger_id, t2.passenger_name
        ORDER BY day_count DESC, passanger_id
```
Na tabuľku flights napojíme všetky ticket_flights a tickety. Získane passenger_id z requestu kontrolujeme
na joinutej tabuľke tickets (preto sme ju joinovali). Necheme user_id preto sme ho dali do podmienky, lebo nechceme lety, kde letel on

V druhom selecte získame všetky flight_id, kde sa nachádzal user v tabuľke tickets.


## Endpoint - detail letu (0,5b)
Požiadavka: GET /v1/bookings/:booking_id <br>
Python:
```shell
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
```
```shell
 SELECT bookings.book_ref as id,
       book_date         as book_date,
       json_build_object(
               'id', t.ticket_no,
               'passenger_id', t.passenger_id,
               'passenger_name', t.passenger_name,
               'boarding_no', bp.boarding_no,
               'flight_no', fl.flight_no,
               'seat', bp.seat_no,
               'aircraft_code', fl.aircraft_code,
               'arrival_airport', fl.arrival_airport,
               'departure_airport', fl.departure_airport,
               'scheduled_arrival', fl.scheduled_arrival,
               'scheduled_departure', fl.scheduled_departure
           )
 FROM bookings.bookings
         LEFT JOIN bookings.tickets t ON bookings.book_ref = t.book_ref
         LEFT JOIN bookings.boarding_passes bp ON t.ticket_no = bp.ticket_no
         LEFT JOIN bookings.flights fl ON bp.flight_id = fl.flight_id
 WHERE bookings.book_ref = '000067'
 ORDER BY t.ticket_no, bp.boarding_no
```
Na rezervácie LEFT joineme tickety, boarding_passes a flights, rovno v dotaze vytvoríme json. Využime
where, kde získame dáta podĺa booking_id.


## Endpoint - detail letu (0,5b)
Požiadavka: GET /v1/flights/late-departure/:delay <br>
Python:
```shell
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
```

SQL:
```shell
  SELECT EXTRACT(EPOCH FROM actual_departure - scheduled_departure)/60 as delay,
            json_build_object(
                'flight_id', flight_id,
                'flight_no', flight_no,
                'delay', ROUND(EXTRACT(EPOCH FROM actual_departure - scheduled_departure)/60, 0)
            )
            FROM bookings.flights
            WHERE EXTRACT(EPOCH FROM actual_departure - scheduled_departure)/60 >= 270
            ORDER BY delay DESC, flight_id
```
Získame všetky lety, kde dĺžka letu je menej ako 270. Vyúiživame funkciu EXTRACT

V PostgreSQL je funkcia EXTRACT(EPOCH) používaná na extrahovanie počtu sekúnd od začiatku epochy (1. január 1970, 00:00:00 UTC) z dátumu / času hodnoty.

Funkcia EXTRACT() sa používa na získanie konkrétnych častí dátumu / času (napríklad rok, mesiac, deň, hodina, minúta atď.). Keď použijete funkciu EXTRACT(EPOCH), vracia počet sekúnd od epochy pre daný dátum / čas.

Napríklad, ak použijeme funkciu EXTRACT(EPOCH) pre dátum "2023-03-12 12:34:56", vráti sa hodnota "1676096096", čo znamená, že od epochy ubehlo 1676096096 sekúnd.

## Endpoint - Linky, ktoré obslúžili najviac pasažierov (0,5b)
Požiadavka: GET /v1/top-airlines?limit=:limit <br>
Python:
```shell
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
```

SQL:
```shell
SELECT count(DISTINCT t.ticket_no) as count, json_build_object(
    'flight_no', flights.flight_no,
    'count', count(t.ticket_no))
        FROM bookings.flights
 LEFT JOIN bookings.ticket_flights tf on flights.flight_id = tf.flight_id
 LEFT JOIN bookings.tickets t on tf.ticket_no = t.ticket_no
        GROUP BY flights.flight_no
 ORDER BY count DESC, flight_no DESC LIMIT 15
```
Získame count ako počet, poďla ktorého zoraďujeme výsledok dopytu a aj ako sekundárne zoradenie poúživame
flight_no. na tabuĺku flights joineme ticket_flights a tickets, aby sme vedeli spočítať všetky tickety.


## Endpoint – naplánované linky (0,5b)
Požiadavka: GET /v1/departures?airport=:airport&day=:day <br>
Python:

```shell
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
```

SQL:
```shell
SELECT json_build_object(
               'flight_id', flight_id,
               'flight_no', flight_no,
               'scheduled_departure', scheduled_departure
            )
            FROM bookings.flights
            WHERE departure_airport = 'KJA'
            AND EXTRACT(ISODOW FROM scheduled_departure) = 7
            AND flights.status = 'Scheduled'
            GROUP BY flight_id
            ORDER BY scheduled_departure, flight_id
```
ISODOW -> získa z dátumu poradie dňa v týždni začinajúce 1 od pondelka.
Podmienka na status letu, deň v týždni a departure_airport.

## Endpoint - Vypíšte všetky destinácie zo zadaného letiska (0,5b)
Požiadavka: GET /v1/airports/:airport/destinations <br>
Python:

```shell
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
```

SQL:
```shell
SELECT airport_code
        FROM bookings.airports_data
        LEFT JOIN bookings.flights f on airports_data.airport_code = f.arrival_airport
        WHERE f.departure_airport = 'VVO'
        GROUP BY airport_code
        ORDER BY airport_code
```
Získame airport_code z airport_data kde sme LEFT JOINLI flights kde flights.departure_airport je kód letiska.


## Endpoint – vyťaženosť letov pre konkrétnu linku (1b)
Požiadavka: GET /v1/airlines/:flight_no/load <br>
Python:

```shell
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
```

SQL:
```shell
SELECT json_build_object('id', flights.flight_id, 'aircraft_capacity',
        (SELECT COUNT(seats.seat_no) FROM seats WHERE aircraft_code = flights.aircraft_code ),
        'load', COUNT(tf.ticket_no), 'percentage_load', ROUND((cast(COUNT(tf.ticket_no) as decimal) / cast(( SELECT COUNT(seats.seat_no) FROM bookings.seats WHERE aircraft_code = flights.aircraft_code ) as decimal) * 100), 2)) FROM bookings.flights
        LEFT JOIN bookings.ticket_flights tf on flights.flight_id = tf.flight_id
        WHERE flights.flight_no = 'PG0242' GROUP BY flights.flight_id ORDER BY flights.flight_id
```
LEFT JOINeme ticket_flights na tabulku k flights tabuľke kvôli získaniu ticket_no a potom v
subquery získame kapacitu (tf.ticket_no) a % počet obsadenosti podielom počtu všetkých sedadiel v lietadle a loadu.

## Priemerná vyťaženosť linky pre jednotlivé dni v týždni (0,5b)
Požiadavka: GET /v1/airlines/:flight_no/load-week
<br>
Python:

```shell
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
```

SQL:
```shell
SELECT
 json_build_object(
        'flight_no', flights.flight_no,
        'monday', (SELECT
        ROUND((SELECT cast(COUNT(tf.ticket_no) as decimal)
                         FROM bookings.flights
                         LEFT JOIN bookings.ticket_flights tf on flights.flight_id = tf.flight_id
                         WHERE flights.flight_no = 'PG0242' AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 1 GROUP BY flights.aircraft_code)
                         / cast(COUNT(s.seat_no) as decimal) * 100, 2)
         FROM bookings.flights
         LEFT JOIN bookings.seats s on flights.aircraft_code = s.aircraft_code
         WHERE flights.flight_no = 'PG0242' AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 1 GROUP BY flights.aircraft_code)
        ,
        'tuesday', (SELECT
        ROUND((SELECT cast(COUNT(tf.ticket_no) as decimal)
                         FROM bookings.flights
                         LEFT JOIN bookings.ticket_flights tf on flights.flight_id = tf.flight_id
                         WHERE flights.flight_no = 'PG0242' AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 2 GROUP BY flights.aircraft_code)
                         / cast(COUNT(s.seat_no) as decimal) * 100, 2)
         FROM bookings.flights
         LEFT JOIN bookings.seats s on flights.aircraft_code = s.aircraft_code
         WHERE flights.flight_no = 'PG0242' AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 2 GROUP BY flights.aircraft_code)
        ,
        'wendesday', (SELECT
        ROUND((SELECT cast(COUNT(tf.ticket_no) as decimal)
                         FROM bookings.flights
                         LEFT JOIN bookings.ticket_flights tf on flights.flight_id = tf.flight_id
                         WHERE flights.flight_no = 'PG0242' AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 3 GROUP BY flights.aircraft_code)
                         / cast(COUNT(s.seat_no) as decimal) * 100, 2)
         FROM bookings.flights
         LEFT JOIN bookings.seats s on flights.aircraft_code = s.aircraft_code
         WHERE flights.flight_no = 'PG0242' AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 3 GROUP BY flights.aircraft_code)
        ,
        'thursday', (SELECT
        ROUND((SELECT cast(COUNT(tf.ticket_no) as decimal)
                         FROM bookings.flights
                         LEFT JOIN bookings.ticket_flights tf on flights.flight_id = tf.flight_id
                         WHERE flights.flight_no = 'PG0242' AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 4 GROUP BY flights.aircraft_code)
                         / cast(COUNT(s.seat_no) as decimal) * 100, 2)
         FROM bookings.flights
         LEFT JOIN bookings.seats s on flights.aircraft_code = s.aircraft_code
         WHERE flights.flight_no = 'PG0242' AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 4 GROUP BY flights.aircraft_code)
        ,
        'friday', (SELECT
        ROUND((SELECT cast(COUNT(tf.ticket_no) as decimal)
                         FROM bookings.flights
                         LEFT JOIN bookings.ticket_flights tf on flights.flight_id = tf.flight_id
                         WHERE flights.flight_no = 'PG0242' AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 5 GROUP BY flights.aircraft_code)
                         / cast(COUNT(s.seat_no) as decimal) * 100, 2)
         FROM bookings.flights
         LEFT JOIN bookings.seats s on flights.aircraft_code = s.aircraft_code
         WHERE flights.flight_no = 'PG0242' AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 5 GROUP BY flights.aircraft_code)
        ,
        'saturday', (SELECT
        ROUND((SELECT cast(COUNT(tf.ticket_no) as decimal)
                         FROM bookings.flights
                         LEFT JOIN bookings.ticket_flights tf on flights.flight_id = tf.flight_id
                         WHERE flights.flight_no = 'PG0242' AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 6 GROUP BY flights.aircraft_code)
                         / cast(COUNT(s.seat_no) as decimal) * 100, 2)
         FROM bookings.flights
         LEFT JOIN bookings.seats s on flights.aircraft_code = s.aircraft_code
         WHERE flights.flight_no = 'PG0242' AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 6 GROUP BY flights.aircraft_code)
        ,
        'sunday', (SELECT
        ROUND((SELECT cast(COUNT(tf.ticket_no) as decimal)
                         FROM bookings.flights
                         LEFT JOIN bookings.ticket_flights tf on flights.flight_id = tf.flight_id
                         WHERE flights.flight_no = 'PG0242' AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 7 GROUP BY flights.aircraft_code)
                         / cast(COUNT(s.seat_no) as decimal) * 100, 2)
         FROM bookings.flights
         LEFT JOIN bookings.seats s on flights.aircraft_code = s.aircraft_code
         WHERE flights.flight_no = 'PG0242' AND EXTRACT(ISODOW FROM flights.scheduled_departure) = 7 GROUP BY flights.aircraft_code)
) FROM bookings.flights
 WHERE flights.flight_no = 'PG0242' GROUP BY flight_no
```
:D Chcelo by to refaktor no ale čo už.
Select na každý deň osobytne, pri každom to funguje tak, že získame pre každý deň kde flight_id
je to čo chceme podľa flight_no zbytočne sa to opakuje ale nevedel som narýchlo prerobit v json_build_object ako to selecnut len raz.
