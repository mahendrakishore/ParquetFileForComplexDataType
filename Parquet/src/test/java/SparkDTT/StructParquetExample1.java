package SparkDTT;

import org.apache.spark.SparkConf;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.*;

public class StructParquetExample1 {
        public static void main(String[] args) {
            System.setProperty("hadoop.home.dir", "C:\\Users\\MKishore\\Downloads\\Hadoop Configuration");
            SparkConf conf = new SparkConf();
            SparkSession spark = SparkSession.builder()
                    .appName("StructParquetExample")
                    .master("local")
                    .getOrCreate();

            // Define the schema for the struct type
            StructType structType = new StructType()
                    .add("name", DataTypes.createMapType(DataTypes.StringType,DataTypes.StringType),true)
                   ;
  Map<String,String> map1 = new HashMap<>();
  map1.put("key1","val1");
  Map<String,String> map2 = new HashMap<>();
  map2.put("key2","val2");
  Map<String,String> map3 = new HashMap<>();
  map3.put("key3","val3");


            // Create a dataset with some sample data
            Row row1 = RowFactory.create(map1);
            Row row2 = RowFactory.create(map2);
            Row row3 = RowFactory.create(map3);

            Dataset<Row> dataset1 = spark.createDataFrame(Arrays.asList(row1,row2,row3), structType);

            dataset1.write().mode(SaveMode.Overwrite).parquet("struct_data.parquet");

        }
    }



