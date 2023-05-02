package SparkDTT;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

import java.util.*;

public class Map1ParquetWriter {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\Users\\MKishore\\Downloads\\Hadoop Configuration");
        SparkConf conf = new SparkConf()
                .setAppName("CreateParquetFileForMapType")
                .setMaster("local[*]");

        SparkContext sc = new SparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<Row> data = (JavaRDD<Row>) Arrays.asList(
                RowFactory.create(new HashMap<String, String>() {{ put("key1","value1");}}),
                RowFactory.create(new HashMap<String, String>() {{ put("key2","value2");}})

        );

        StructType schema = new StructType().add("map_col", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));

        Dataset<Row> df = sqlContext.createDataFrame(data, schema);

        df.write().mode(SaveMode.Overwrite).parquet("map_data.parquet");

        sc.stop();
    }
    }


