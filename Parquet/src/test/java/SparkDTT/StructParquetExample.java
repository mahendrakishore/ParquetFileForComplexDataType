package SparkDTT;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.util.Arrays;

public class StructParquetExample {
        public static void main(String[] args) {
            System.setProperty("hadoop.home.dir", "C:\\Users\\MKishore\\Downloads\\Hadoop Configuration");
            SparkConf conf = new SparkConf();
            SparkSession spark = SparkSession.builder()
                    .appName("StructParquetExample")
                    .master("local")
                    .getOrCreate();

            // Define the schema for the struct type
            StructType structType = new StructType()
                    .add("name", DataTypes.StringType)
                    .add("age", DataTypes.IntegerType)
                    .add("gender", DataTypes.StringType);

            // Create a dataset with some sample data
            Row row1 = RowFactory.create("John Doe", 25, "Male");
            Row row2 = RowFactory.create("Jane Doe", 27, "Female");
            Row row3 = RowFactory.create("Bob Smith", 30, "Male");
            Dataset<Row> dataset = spark.createDataFrame(Arrays.asList(row1, row2, row3), structType);

            // Write the dataset to a Parquet file
            //String outputDir = "/path/to/output/dir/struct";
            dataset.write().mode(SaveMode.Overwrite).parquet("struct_data.parquet");
        }
    }



