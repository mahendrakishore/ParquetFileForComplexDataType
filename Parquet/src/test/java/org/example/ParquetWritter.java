/*
package org.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class ParquetWritter extends ParquetWriter<String[]> {

   // private static final Logger LOGGER = LoggerFactory.getLogger(ParquetWritter.class);
   public static final String SCHEMA_LOCATION = "/src/test/java/org/example/avroToParquet.avsc";
   public static final Path OUT_PATH = new Path("/src/test/java/org/example/sample.parquet");

    public ParquetWritter(
            Path file,
            MessageType schema,
            boolean enableDictionary,
            CompressionCodecName codecName
    ) throws IOException {
        super(file, new CustomWriteSupport(schema), codecName, DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE, enableDictionary, false);
    }



    public static void main(String[] args) throws IOException {}

}
*/
