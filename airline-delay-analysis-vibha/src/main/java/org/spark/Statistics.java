package org.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.utils.Config;
import org.utils.Constants;

import static org.apache.spark.sql.functions.*;

public class Statistics {

    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        String inputPath = Config.get(Constants.INPUT_PATH) + "Airline_Delay_Cause.csv";
        Dataset<Row> df = loadCsv(spark, inputPath);

        // Task 1.5 - Profiling Summary
        printSchema(df);
        printRowCount(df);
        printNullCounts(df);
        printNumericStats(df);
        printDistinctSamples(df);

        spark.stop();
    }

    // Spark session
    private static SparkSession createSparkSession() {
        return SparkSession.builder()
                .appName("Dataset Explorer")
                .master("local[*]")
                .getOrCreate();
    }

    // Load dataset
    private static Dataset<Row> loadCsv(SparkSession spark, String path) {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(path);
    }

    // Print schema
    private static void printSchema(Dataset<Row> df) {
        System.out.println("===== Schema =====");
        df.printSchema();
    }

    // Print row count
    private static void printRowCount(Dataset<Row> df) {
        long rowCount = df.count();
        System.out.println("\n===== Total Rows: " + rowCount + " =====");
    }

    // Print null counts for important columns
    private static void printNullCounts(Dataset<Row> df) {
        String[] cols = {
                "arr_delay", "arr_flights", "carrier_delay", "weather_delay",
                "nas_delay", "security_delay", "late_aircraft_delay",
                "carrier", "airport"
        };

        System.out.println("\n===== Null/Empty Counts =====");
        for (String c : cols) {
            long nullCount = df.filter(col(c).isNull().or(col(c).equalTo(""))).count();
            System.out.println(c + " -> " + nullCount);
        }
    }

    // Print min, max, avg for numeric columns
    private static void printNumericStats(Dataset<Row> df) {
        String[] numCols = {
                "arr_delay", "carrier_delay", "weather_delay",
                "nas_delay", "security_delay", "late_aircraft_delay"
        };

        System.out.println("\n===== Numeric Columns Stats (min, max, avg) =====");
        for (String c : numCols) {
            Dataset<Row> stats = df.agg(
                    min(c).alias("min_" + c),
                    max(c).alias("max_" + c),
                    avg(c).alias("avg_" + c)
            );
            stats.show(false);
        }
    }

    // Print distinct samples for categorical columns
    private static void printDistinctSamples(Dataset<Row> df) {
        System.out.println("\n===== Sample Carriers =====");
        df.select("carrier").distinct().show(10, false);

        System.out.println("\n===== Sample Airports =====");
        df.select("airport").distinct().show(10, false);
    }
}
