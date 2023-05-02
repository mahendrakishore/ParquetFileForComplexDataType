/*
package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

    public class ParquetFileWriterExampleNew {

        public static void main(String[] args) throws IOException {

            // Define the schema for the Parquet file
            Type[] fields = new Type[] {
                    Types.required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("id"),
                    Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("timestamp"),
                    Types.repeated(PrimitiveType.PrimitiveTypeName.DOUBLE).named("values")
            };
            MessageType schema = new MessageType("my_parquet_file", fields);

            // Create a Parquet writer object to write data to the file
            Configuration conf = new Configuration();
            Path path = new Path("my_parquet_file.parquet");
            GroupWriteSupport.setSchema(schema, conf);
            GroupWriteSupport groupWriteSupport = new GroupWriteSupport();
            ParquetWriter<Group> writer = new ParquetWriter<Group>(
                    path,
                    new GroupWriteSupport(),
                    CompressionCodecName.SNAPPY,
                    128 * 1024 * 1024,
                    1 * 1024 * 1024,
                    4 * 1024 * 1024,
                    true,
                    false,
                    WriterVersion.PARQUET_2_0,
                    conf);

            // Create an array of values for each row of data
            List<SimpleGroup> data = new ArrayList<>();
            data.add(createSimpleGroup("row1", 123456789L, new double[] {1.0, 2.0, 3.0}));
            data.add(createSimpleGroup("row2", 123456790L, new double[] {4.0, 5.0, 6.0}));

            // Write each row of data to the Parquet file
            for (SimpleGroup row : data) {
                writer.write(row);
            }

            // Close the writer to finish writing the file
            writer.close();
        }

        // Helper method to create a SimpleGroup object from the data

        }
*/
