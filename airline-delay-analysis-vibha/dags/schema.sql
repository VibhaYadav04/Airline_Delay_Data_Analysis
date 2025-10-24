-- Create Databases
CREATE DATABASE IF NOT EXISTS bronze;

-- =====================
-- BRONZE LAYER
-- =====================

--
CREATE TABLE IF NOT EXISTS bronze.raw_dataset (
	year            INT , -- Year (YYYY)
    month           INT, -- Month (1–12)
    carrier         VARCHAR(50), -- US DOT airline carrier code
    carrier_name    VARCHAR(255), -- Full airline name under DOT certificate
    airport         VARCHAR(50), -- Airport IATA/DOT code
    airport_name    VARCHAR(255), -- Full airport name
    arr_flights     INT, -- Number of arrival flights scheduled
    arr_del15       INT, -- Count of flights delayed ≥15 minutes
    carrier_ct      INT, -- Number of flights delayed due to carrier
    weather_ct      INT, -- Number of flights delayed due to weather
    nas_ct          INT, -- Number of flights delayed due to NAS (airspace)
    security_ct     INT, -- Number of flights delayed due to security
    late_aircraft_ct INT, -- Number of flights delayed due to late-arriving aircraft
    arr_cancelled   INT, -- Flights cancelled
    arr_diverted    INT, -- Flights diverted
    arr_delay       DOUBLE, -- Difference in minutes between scheduled and actual arrival (negative = early)
    carrier_delay   DOUBLE, -- Delay minutes due to carrier
    weather_delay   DOUBLE, -- Delay minutes due to weather
    nas_delay       DOUBLE, -- Delay minutes due to NAS
    security_delay  DOUBLE, -- Delay minutes due to security
    late_aircraft_delay DOUBLE -- Delay minutes due to late-arriving aircraft
    );
--
CREATE TABLE IF NOT EXISTS bronze.carrier_lookup (
    carrier       VARCHAR(50), -- US DOT airline carrier code
    carrier_name  VARCHAR(255) -- Full airline name
    );
--
CREATE TABLE IF NOT EXISTS bronze.airport_lookup (
    iso_country   VARCHAR(50), -- Country code of airport (e.g., US)
    name          VARCHAR(50), -- Full airport name
    iata_code     VARCHAR(255) -- IATA airport code
    );

-- =====================
-- SILVER LAYER
-- =====================
--
CREATE DATABASE IF NOT EXISTS silver;


CREATE TABLE IF NOT EXISTS silver.cleaned_dataset (
	year             INT, -- YYYY
	month            INT, -- Range 1–12 enforced
    carrier          VARCHAR(50), -- Trimmed, uppercased
    carrier_name     VARCHAR(255), -- Trimmed, uppercased
    airport          VARCHAR(50), -- Trimmed, uppercased
    airport_name     VARCHAR(255), -- Trimmed, uppercased
    arr_flights      INT, -- Casted to INT, rows with NULL dropped, Greater than 0 filtered
    arr_del15        INT, -- Count of flights delayed ≥15 minutes
    carrier_ct       INT, -- Number of flights delayed due to carrier
    weather_ct       INT, -- Number of flights delayed due to weather
    nas_ct           INT, -- Number of flights delayed due to NAS (airspace)
    security_ct      INT, -- Number of flights delayed due to security
    late_aircraft_ct INT, -- Number of flights delayed due to late-arriving aircraft
    arr_cancelled    INT, -- Flights cancelled
    arr_diverted     INT, -- Flights diverted
    arr_delay        DOUBLE, -- Casted to DOUBLE, rows with NULL dropped, Greater than 0 filtered
    carrier_delay    DOUBLE, -- Delay minutes due to carrier
    weather_delay    DOUBLE, -- Delay minutes due to weather
    nas_delay        DOUBLE, -- Delay minutes due to NAS
    security_delay   DOUBLE, -- Delay minutes due to security
    late_aircraft_delay DOUBLE, -- Delay minutes due to late-arriving aircraft
    date             DATE -- Derived field for analysis (constructed from ‘01’ + month + year)
    );
--
CREATE TABLE IF NOT EXISTS silver.cleaned_carrier_lookup (
	carrier       VARCHAR(50), -- Airline carrier code
    carrier_name  VARCHAR(255) -- Standardized full airline name
    );
--
CREATE TABLE IF NOT EXISTS silver.cleaned_airport_lookup (
    iso_country   VARCHAR(50), -- Country code (standardized, uppercased)
    name          VARCHAR(255), -- Standardized airport name
    iata_code     VARCHAR(50) -- Standardized airport code
    );

-- =====================
-- GOLD LAYER
-- =====================

--
CREATE DATABASE IF NOT EXISTS gold;
CREATE TABLE IF NOT EXISTS gold.dim_carrier (
	carrier_id    INT AUTO_INCREMENT PRIMARY KEY, -- Surrogate key (auto-increment)
	carrier       VARCHAR(50) UNIQUE, -- Airline carrier code
    carrier_name  VARCHAR(50) -- Full airline name
    );

--
CREATE TABLE IF NOT EXISTS gold.dim_airport (
	airport_id    INT AUTO_INCREMENT PRIMARY KEY, -- Surrogate key (auto-increment)
	airport       VARCHAR(50) UNIQUE, -- Airport IATA code
    airport_name  VARCHAR(255) -- Full airport name
    );


--
CREATE TABLE IF NOT EXISTS gold.fact_flight_performance (
	flight_id            INT AUTO_INCREMENT PRIMARY KEY, -- Surrogate key (auto-increment)
	year                 INT, -- YYYY
	month                INT, -- Month (1-12)
	airport_id           INT, -- Reference to dim_airport (FK)
	carrier_id           INT, -- Reference to dim_carrier (FK)
	arr_flights          INT, -- Number of arrival flights scheduled
    arr_delay            DOUBLE, -- Difference (minutes) scheduled vs actual arrival
	arr_del15            INT, -- Flights delayed ≥15 minutes or less
	carrier_ct           INT, -- Count of delays caused by carrier
	weather_ct           INT, -- Count of delays caused by weather
	nas_ct               INT, -- Count of delays caused by NAS
	security_ct          INT, -- Count of delays caused by security
    late_aircraft_ct     INT, -- Count of delays caused by late-arriving aircraft
	carrier_delay        DOUBLE, -- Delay minutes due to carrier
	weather_delay        DOUBLE, -- Delay minutes due to weather
	nas_delay            DOUBLE, -- Delay minutes due to NAS
	security_delay       DOUBLE, -- Delay minutes due to security
	late_aircraft_delay  DOUBLE, -- Delay minutes due to late-arriving aircraft
	arr_cancelled        INT, -- Flights cancelled
	arr_diverted         INT, -- Flights diverted
	total_delay          DOUBLE, -- Derived: sum of all delay minutes
	on_time_flag         INT, -- Derived:  if arr_del15 < 15 then 1,  else 0
	date                 DATE,
    FOREIGN KEY (airport_id) REFERENCES gold.dim_airport(airport_id),
    FOREIGN KEY (carrier_id) REFERENCES gold.dim_carrier(carrier_id)
    );
