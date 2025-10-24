-- On Time Performance Queries
USE GOLD;
-- Task 6.1 - % of delayed flights
SELECT
    (SUM(arr_del15) * 100.0 / SUM(arr_flights)) AS delayed_flight_percentage
FROM fact_flight_performance;

-- Task 6.2 - On-time performance by carrier
SELECT
    c.carrier_name,
    SUM(f.arr_del15) AS delayed_flights,
    SUM(f.arr_flights) AS total_flights,
    (SUM(f.arr_del15) * 100.0 / SUM(f.arr_flights)) AS delay_percentage
FROM fact_flight_performance f
         JOIN dim_carrier c
              ON f.carrier_id = c.carrier_id
GROUP BY c.carrier_name
ORDER BY delay_percentage DESC;

-- Task 6.3 - On-time performance by airport
SELECT
    a.airport_name,
    SUM(f.arr_del15) AS delayed_flights,
    SUM(f.arr_flights) AS total_flights,
    (SUM(f.arr_del15) * 100.0 / SUM(f.arr_flights)) AS delay_percentage
FROM fact_flight_performance f
         JOIN dim_airport a
              ON f.airport_id = a.airport_id
GROUP BY a.airport_name
ORDER BY delay_percentage DESC;

-- Task 6.4 - Monthly performance for one airline 'AA'
SELECT
    f.year,
    f.month,
    SUM(f.arr_del15) AS delayed_flights,
    SUM(f.arr_flights) AS total_flights,
    (SUM(f.arr_del15) * 100.0 / SUM(f.arr_flights)) AS delay_percentage
FROM fact_flight_performance f
         JOIN dim_carrier c
              ON f.carrier_id = c.carrier_id
WHERE c.carrier = 'AA'
GROUP BY f.year, f.month
ORDER BY f.year, f.month;
