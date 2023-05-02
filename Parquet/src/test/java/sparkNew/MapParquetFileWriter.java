/*
package sparkNew;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class MapParquetFileWriter {


    public static void main(String[] args) {

        // create SparkSession
        SparkSession spark = SparkSession.builder().appName("ParquetFileWriter").getOrCreate();

        // create schema for the struct data type
        StructType schema = new StructType()
                .add("name", DataTypes.StringType)
                .add("age", DataTypes.IntegerType)
                .add("city", DataTypes.StringType);

        // create sample data as a List of Rows
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create("John", 25, "New York"));
        rows.add(RowFactory.create("Jane", 30, "San Francisco"));
        rows.add(RowFactory.create("Bob", 40, "Chicago"));
        Dataset<Row> data = spark.createDataFrame(rows, schema);

        // write data to a Parquet file
        String outputPath = "path/to/parquet/file";
        data.write().parquet(outputPath);

        // stop SparkSession
        spark.stop();
    }
}
*/
