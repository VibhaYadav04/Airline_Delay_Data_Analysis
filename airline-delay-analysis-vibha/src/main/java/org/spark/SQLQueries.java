package org.spark;

import org.apache.spark.sql.*;
import org.exception.GlobalExceptionHandler;
import org.utils.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.utils.Constants;

public class SQLQueries {
    private static final Logger logger = LoggerFactory.getLogger(SQLQueries.class);

    public static void main(String[] args) {
        SparkSession spark = null;
        try {
            spark = createSparkSession();

            String goldUrl      = Config.getDbUrl(Constants.GOLD_SCHEMA);
            String goldUser     = Config.getDbUser();
            String goldPassword = Config.getDbPassword();
            String goldDriver   = Config.getDbDriver();

            // Load from Gold Database
            Dataset<Row> factFlights = loadTable(spark, "fact_flight_performance",
                    goldUrl, goldUser, goldPassword, goldDriver);
            Dataset<Row> carrierDimDb = loadTable(spark, "dim_carrier",
                    goldUrl, goldUser, goldPassword, goldDriver);
            Dataset<Row> airportDimDb = loadTable(spark, "dim_airport",
                    goldUrl, goldUser, goldPassword, goldDriver);

            // Register as temp views
            factFlights.createOrReplaceTempView("fact_flight_performance");
            carrierDimDb.createOrReplaceTempView("dim_carrier");
            airportDimDb.createOrReplaceTempView("dim_airport");

            // ---------------- Task 6 Queries ----------------

            // Task 6.1 - % of delayed flights
            logger.info("Task 6.1 - % of delayed flights");
            runAndLog(spark, "SELECT (SUM(arr_del15) * 100.0 / SUM(arr_flights)) AS delayed_flight_percentage " +
                    "FROM fact_flight_performance");

            // Task 6.2 - On-time performance by carrier
            logger.info("Task 6.2 - On-time performance by carrier");
            runAndLog(spark, "SELECT c.carrier_name, " +
                    "SUM(f.arr_del15) AS delayed_flights, " +
                    "SUM(f.arr_flights) AS total_flights, " +
                    "(SUM(f.arr_del15) * 100.0 / SUM(f.arr_flights)) AS delay_percentage " +
                    "FROM fact_flight_performance f " +
                    "JOIN dim_carrier c ON f.carrier_id = c.carrier_id " +
                    "GROUP BY c.carrier_name " +
                    "ORDER BY delay_percentage DESC");

            // Task 6.3 - On-time performance by airport
            logger.info("Task 6.3 - On-time performance by airport");
            runAndLog(spark, "SELECT a.airport_name, " +
                    "SUM(f.arr_del15) AS delayed_flights, " +
                    "SUM(f.arr_flights) AS total_flights, " +
                    "(SUM(f.arr_del15) * 100.0 / SUM(f.arr_flights)) AS delay_percentage " +
                    "FROM fact_flight_performance f " +
                    "JOIN dim_airport a ON f.airport_id = a.airport_id " +
                    "GROUP BY a.airport_name " +
                    "ORDER BY delay_percentage DESC");

            // Task 6.4 - Monthly performance for one airline "AA"
            logger.info("Task 6.4 - Monthly performance for airline 'AA'");
            runAndLog(spark, "SELECT f.year, f.month, " +
                    "SUM(f.arr_del15) AS delayed_flights, " +
                    "SUM(f.arr_flights) AS total_flights, " +
                    "(SUM(f.arr_del15) * 100.0 / SUM(f.arr_flights)) AS delay_percentage " +
                    "FROM fact_flight_performance f " +
                    "JOIN dim_carrier c ON f.carrier_id = c.carrier_id " +
                    "WHERE c.carrier = 'AA' " +
                    "GROUP BY f.year, f.month " +
                    "ORDER BY f.year, f.month");

            // ---------------- Task 7 Queries ----------------

            // Task 7.1 - Total delay by cause
            logger.info("Task 7.1 - Total delay by cause");
            runAndLog(spark, "SELECT " +
                    "SUM(carrier_ct) AS carrier_delay_count, " +
                    "SUM(weather_ct) AS weather_delay_count, " +
                    "SUM(nas_ct) AS nas_delay_count, " +
                    "SUM(security_ct) AS security_delay_count, " +
                    "SUM(late_aircraft_ct) AS late_aircraft_delay_count " +
                    "FROM fact_flight_performance");

            // Task 7.2 - Total delay by minutes
            logger.info("Task 7.2 - Total delay by minutes");
            runAndLog(spark, "SELECT " +
                    "SUM(carrier_delay) AS carrier_delay_minutes, " +
                    "SUM(weather_delay) AS weather_delay_minutes, " +
                    "SUM(nas_delay) AS nas_delay_minutes, " +
                    "SUM(security_delay) AS security_delay_minutes, " +
                    "SUM(late_aircraft_delay) AS late_aircraft_delay_minutes " +
                    "FROM fact_flight_performance");

            // Task 7.3 - Average delay by minutes
            logger.info("Task 7.3 - Average delay by minutes");
            runAndLog(spark, "SELECT (SUM(arr_delay) * 1.0 / SUM(arr_del15)) AS avg_delay_per_delayed_flight " +
                    "FROM fact_flight_performance " +
                    "WHERE arr_del15 > 0");

            // Task 7.4 - Yearly delay trend
            logger.info("Task 7.4 - Yearly delay trend");
            runAndLog(spark, "SELECT year, " +
                    "SUM(carrier_delay + weather_delay + nas_delay + security_delay + late_aircraft_delay) AS total_delay_minutes, " +
                    "SUM(carrier_ct + weather_ct + nas_ct + security_ct + late_aircraft_ct) AS total_delayed_flights, " +
                    "ROUND((SUM(carrier_delay) + SUM(weather_delay) + SUM(nas_delay) + SUM(security_delay) + SUM(late_aircraft_delay)) / SUM(arr_del15), 2) AS average_delay_per_delayed_flight " +
                    "FROM fact_flight_performance " +
                    "WHERE year >= 2000 " +
                    "GROUP BY year " +
                    "ORDER BY year");

            // Task 7.5 - Cancellations by carrier & year
            logger.info("Task 7.5 - Cancellations by carrier & year");
            runAndLog(spark, "SELECT c.carrier, c.carrier_name, f.year, " +
                    "SUM(f.arr_cancelled) AS total_cancellations, " +
                    "SUM(f.arr_diverted) AS total_diversions " +
                    "FROM fact_flight_performance f " +
                    "JOIN dim_carrier c ON f.carrier_id = c.carrier_id " +
                    "GROUP BY c.carrier, c.carrier_name, f.year " +
                    "ORDER BY f.year, c.carrier");

        } catch (Exception e) {
            GlobalExceptionHandler.handle(e, "Fact loading failed");
        } finally {
            if (spark != null) {
                spark.stop();
            }
        }
    }

    // Spark Session
    private static SparkSession createSparkSession() {
        return SparkSession.builder()
                .appName("Airline SQL Queries")
                .master("local[*]")
                .getOrCreate();
    }

    // Load table from DB
    private static Dataset<Row> loadTable(SparkSession spark, String tableName,
                                          String url, String user, String password, String driver) {
        logger.info("Loading table: {}", tableName);
        return spark.read()
                .format("jdbc")
                .option("url", url)
                .option("dbtable", tableName)
                .option("user", user)
                .option("password", password)
                .option("driver", driver)
                .load();
    }

    // Helper: run SQL + log results
    private static void runAndLog(SparkSession spark, String sql) {
        Dataset<Row> result = spark.sql(sql);
        result.collectAsList().forEach(row -> logger.info(row.toString()));
    }
}
