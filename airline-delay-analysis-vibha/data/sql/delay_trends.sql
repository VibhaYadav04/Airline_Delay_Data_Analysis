USE GOLD;
-- Task 7.1 Total delay by cause

SELECT
    SUM(carrier_ct) AS carrier_delay_count,
    SUM(weather_ct) AS weather_delay_count,
    SUM(nas_ct) AS nas_delay_count,
    SUM(security_ct) AS security_delay_count,
    SUM(late_aircraft_ct) AS late_aircraft_delay_count
FROM fact_flight_performance;

-- Task 7.2 Total delay by minutes
SELECT
    SUM(carrier_delay) AS carrier_delay_minutes,
    SUM(weather_delay) AS weather_delay_minutes,
    SUM(nas_delay) AS nas_delay_minutes,
    SUM(security_delay) AS security_delay_minutes,
    SUM(late_aircraft_delay) AS late_aircraft_delay_minutes
FROM fact_flight_performance;

-- Task 7.3 Average delay by minutes
SELECT
    (SUM(arr_delay) * 1.0 / SUM(arr_del15)) AS avg_delay_per_delayed_flight
FROM fact_flight_performance
WHERE arr_del15 > 0;

-- Task 7.4 Yearly delay trend
SELECT
    year,
    SUM(carrier_delay + weather_delay + nas_delay + security_delay + late_aircraft_delay) AS total_delay_minutes,
    SUM(carrier_ct + weather_ct + nas_ct + security_ct + late_aircraft_ct) AS total_delayed_flights,
    ROUND(SUM(carrier_delay + weather_delay + nas_delay + security_delay + late_aircraft_delay) / SUM(arr_del15), 2) AS average_delay_per_delayed_flight
FROM fact_flight_performance
WHERE year >= 2000
GROUP BY year
ORDER BY year;

-- Task 7.5 Cancellations by carrier & year
SELECT
    c.carrier,
    c.carrier_name,
    f.year,
    SUM(f.arr_cancelled) AS total_cancellations,
    SUM(f.arr_diverted) AS total_diversions
FROM fact_flight_performance f
         JOIN dim_carrier c ON f.carrier_id = c.carrier_id
GROUP BY c.carrier, c.carrier_name, f.year
ORDER BY f.year, c.carrier;
