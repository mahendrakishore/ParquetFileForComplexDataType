/*
package SparkDTT;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.HashMap;
import java.util.Map;

    public class MapVALUE {

        public static void main(String[] args) {

            // Create a SparkSession
            SparkSession spark = SparkSession.builder()
                    .appName("Spark Map DataFrame Example")
                    .master("local[*]")
                    .getOrCreate();

            // Create a Map of data
            Map<String, Integer> dataMap = new HashMap<>();
            dataMap.put("John", 25);
            dataMap.put("Jane", 30);
            dataMap.put("Mike", 35);

            // Convert the Map to a DataFrame
            Dataset<Row> df = spark.createDataFrame(
                    spark.sparkContext().parallelize(dataMap.entrySet()),
                    functions.schemaOf(
                            functions.createMapType(
                                    functions.DataTypes.StringType,
                                    functions.DataTypes.IntegerType
                            )
                    )
            ).toDF("name", "age");

            // Show the DataFrame
            df.show();

            // Stop the SparkSession
            spark.stop();
        }
    }

}
*/
