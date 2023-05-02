package SparkDTT;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class ArrayParquetWriter1 {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\Users\\MKishore\\Downloads\\Hadoop Configuration");
        SparkConf conf = new SparkConf()
                .setAppName("CreateParquetFileForMapType")
                .setMaster("local[*]");

        SparkContext sc = new SparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Row[] data = new Row[]{
                RowFactory.create(new String[]{"apple"}),
                RowFactory.create(new String[]{"grape"}),
                RowFactory.create(new String[]{"watermelon"})
        };

        StructType schema = new StructType(new StructField[] {
               DataTypes.createStructField("data", DataTypes.createArrayType(DataTypes.StringType, true), false)
        });
        Dataset<Row> df = sqlContext.createDataFrame(Arrays.asList(data), schema);

        df.write().mode(SaveMode.Overwrite).parquet("map_data.parquet");

        sc.stop();
    }
}