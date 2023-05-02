package SparkDTT;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.*;

public class MapParquetWriter {


    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\Users\\MKishore\\Downloads\\Hadoop Configuration");
            SparkConf conf = new SparkConf()
                    .setAppName("CreateParquetFileForMapType")
                    .setMaster("local[*]");

            SparkContext sc = new SparkContext(conf);
            SQLContext sqlContext = new SQLContext(sc);

            List<Row> data = Arrays.asList(
                    RowFactory.create(
                            new HashMap<String, String>() {{
                                put("key1", "value1");
                                put("key2", "value2");
                                put("key3", "value3");
                                put("key3", "value4");
                            }}
                    ));

            StructType schema = new StructType().add("map_col", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));

            Dataset<Row> df = sqlContext.createDataFrame(data, schema);

            df.write().mode(SaveMode.Overwrite).parquet("map_data.parquet");

            sc.stop();
        }
    }


