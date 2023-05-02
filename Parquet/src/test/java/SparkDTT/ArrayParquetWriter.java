package SparkDTT;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang3.SystemUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.functions.*;

public class ArrayParquetWriter {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\Users\\MKishore\\Downloads\\Hadoop Configuration");
        SparkSession spark = SparkSession.builder().appName("ArrayParquetWriter").master("local")
                .getOrCreate();

        // Define a schema for the data
        StructType schema = new StructType(new StructField[] {
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("data", new ArrayType(DataTypes.StringType, true), false)
        });

        // Create some sample data
        Row[] data = new Row[]{
                RowFactory.create(1, new String[]{"apple", "banana", "orange"}),
                RowFactory.create(2, new String[]{"grape", "pear"}),
                RowFactory.create(3, new String[]{"watermelon", "kiwi", "mango"})
        };

        // Create a DataFrame from the sample data and the schema
        Dataset<Row> df = spark.createDataFrame(Arrays.asList(data), schema);

        // Write the DataFrame to a Parquet file
        df.write().mode(SaveMode.Overwrite).parquet("array_data1.parquet");

        spark.stop();
    }
}
