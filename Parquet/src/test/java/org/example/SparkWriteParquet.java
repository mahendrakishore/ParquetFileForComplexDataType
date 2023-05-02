package org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class SparkWriteParquet {

    public static void main(String[] args) {
        try {

            /* Creating Spark Session */
            SparkSession sparkSession = SparkSession.builder().appName("SparkWriteParquet").master("local").getOrCreate();
//Creating dataset using list of row of spark.sql *
            List<Row> listRows = new ArrayList<>();
            listRows.add(RowFactory.create("Joy", "Indore"));

            Dataset<Row> dataset = sparkSession.createDataset(listRows, getEncoder());
            dataset.show();
// write parquet file using write & savemode metho

            dataset.coalesce(1).write().mode(SaveMode.Overwrite).option("header", true).parquet((SparkWriteParquet.class.getClassLoader().getResource("").getPath() + "tmp" + Instant.now().getNano() + ".parquet"));

            System.out.println("PARQUET file generated successfully");
//Also we can use save method to write parquet file


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static ExpressionEncoder<Row> getEncoder() {
        StructType structType = new StructType();
        structType = structType.add("name", DataTypes.StringType, false);
        structType = structType.add("city", DataTypes.StringType, false);
        return RowEncoder.apply(structType);
    }
}
