/*
package SparkDTT;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.ListMap;

import java.util.*;

import static org.apache.spark.sql.functions.col;

public class Map2ParquetWriter {

    public static void main(String[] args) {

        // Configure Spark
        SparkConf conf = new SparkConf().setAppName("MapTypeExample").setMaster("local[*]");
        SparkContext sc = new SparkContext(conf);
        SparkSession spark = new SparkSession.Builder().appName("MapTypeExample").getOrCreate();

        // Create a DataFrame with a column of MapType
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("properties", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), false)
        });
        HashMap<String, String> properties1 = new HashMap<>();
        properties1.put("color", "red");
        properties1.put("size", "small");
        HashMap<String, String> properties2 = new HashMap<>();
        properties2.put("color", "blue");
        properties2.put("size", "medium");
        HashMap<String, String> properties3 = new HashMap<>();
        properties3.put("color", "green");
        properties3.put("size", "large");
        Row row1 = RowFactory.create(1, JavaConverters.mapAsScalaMap(properties1).toMap(scala.Predef.<Tuple2<String, String>>conforms()));
        Row row2 = RowFactory.create(2, JavaConverters.mapAsScalaMap(properties2).toMap(scala.Predef.<Tuple2<String, String>>conforms()));
        Row row3 = RowFactory.create(3, JavaConverters.mapAsScalaMap(properties3).toMap(scala.Predef.<Tuple2<String, String>>conforms()));
        Dataset<Row> data = spark.createDataFrame(Arrays.asList(row1, row2, row3), schema);

        // Define a UDF to extract a value from the MapType
        UserDefinedFunction getValue = functions.udf((UDF1<Map<String, String>, String>) map -> map.get("color"), DataTypes.StringType);

        // Use the UDF to extract a value from the MapType column
        Dataset<Row> result = data.select(col("id"), getValue.apply(col("properties")).as("color"));

        // Show the result
        result.show();

        // Stop Spark
        spark.stop();
    }
}
    }


*/
