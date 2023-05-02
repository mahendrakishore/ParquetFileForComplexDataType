package SparkDTT;


    import org.apache.spark.SparkConf;
    import org.apache.spark.SparkContext;
    import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
    import org.apache.spark.sql.*;

    import org.apache.spark.sql.types.DataTypes;
    import org.apache.spark.sql.types.StructType;
    import scala.Tuple2;

    import java.util.*;

public class NewMap {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\Users\\MKishore\\Downloads\\Hadoop Configuration");
        // create SparkSession
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .config("spark.io.compression.codec", "snappy")
                .appName("ParquetFileWriter")
                .getOrCreate();

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
        Dataset<Row> df = spark.createDataFrame(rows, schema);

        // write data to a Parquet file
        String outputPath = "path/to/parquet/file";
        df.write().parquet("path/to/parquet/file");

        // stop SparkSession
        spark.stop();
    }
}