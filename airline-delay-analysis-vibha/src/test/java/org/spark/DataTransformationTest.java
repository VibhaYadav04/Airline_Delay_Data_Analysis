package org.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class DataTransformationTest {

    private static SparkSession spark;

    @BeforeAll
    public static void setup() {
        spark = SparkSession.builder()
                .appName("DataTransformationTest")
                .master("local[*]")
                .getOrCreate();
    }

    @AfterAll
    public static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    public void testAddDerivedFields() {
        // Sample schema
        StructType schema = new StructType(new StructField[]{
                new StructField("arr_delay", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("carrier_delay", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("weather_delay", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("nas_delay", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("security_delay", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("late_aircraft_delay", DataTypes.IntegerType, true, Metadata.empty())
        });

        // Sample data
        List<Row> rows = Arrays.asList(
                RowFactory.create(10, 5, 0, null, 0, 0),   // arr_delay < 15 → on_time_flag=1
                RowFactory.create(20, null, 10, 5, null, 2) // arr_delay >= 15 → on_time_flag=0
        );

        Dataset<Row> inputDf = spark.createDataFrame(rows, schema);

        // Apply derived fields function
        Dataset<Row> resultDf = DataTransformation.addDerivedFields(inputDf);

        List<Row> results = resultDf.collectAsList();

        // First row checks
        Row row1 = results.get(0);
        assertEquals(1, (int) row1.getAs("on_time_flag"));
        assertEquals(5, (int) row1.getAs("total_delay"));

        // Second row checks
        Row row2 = results.get(1);
        assertEquals(0, (int) row2.getAs("on_time_flag"));
        assertEquals(17, (int) row2.getAs("total_delay"));

    }
}
