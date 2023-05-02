/*
package org.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class AvroParquet {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroParquet.class);

    private static final Schema SCHEMA;

    //private static final String SCHEMA_LOCATION = "src/test/resource/avroToParquet.avsc";
    // private static final String SCHEMA_LOCATION = AvroParquet.class.getResource("avroToParquet.avsc").getPath();
    private static final Path OUT_PATH = new Path("src/test/resources/sample.parquet");

    static {
        try (InputStream inStream = AvroParquet.class.getClassLoader().getResourceAsStream("avroToParquet.avsc")) {
            SCHEMA = new Schema.Parser().parse(IOUtils.toString(inStream, "UTF-8"));
        } catch (Exception e) {
            //LOGGER.error("Can't read SCHEMA file from {}", SCHEMA_LOCATION);
            throw new RuntimeException("Can't read SCHEMA file from" + "src/test/resource/avroToParquet.avsc", e);
        }
    }

    public static void main(String[] args) throws IOException {
        System.setProperty("hadoop.home.dir", "C:\\Users\\MKishore\\Downloads\\Hadoop Configuration");
        List<GenericData.Record> sampleData = new ArrayList<>();

        GenericData.Record record = new GenericData.Record(SCHEMA);
        record.put("c1", 1);
        sampleData.add(record);

        AvroParquet writerReader = new AvroParquet();
        writerReader.writeToParquet(sampleData, new Path(AvroParquet.class.getClassLoader().getResource("").getPath() + "tmp" + Instant.now().getNano() + ".parquet"));
        //writerReader.readFromParquet(OUT_PATH);

    }

    @SuppressWarnings("unchecked")
    public void readFromParquet(Path filePathToRead) throws IOException {
        try (ParquetReader<GenericData.Record> reader = AvroParquetReader
                .<GenericData.Record>builder(filePathToRead)
                .withConf(new Configuration())
                .build()) {

            GenericData.Record record;
            while ((record = reader.read()) != null) {
                System.out.println(record);
            }
        }
    }

    public void writeToParquet(List<GenericData.Record> recordsToWrite, Path fileToWrite) throws IOException {
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter
                .<GenericData.Record>builder(fileToWrite)
                .withSchema(SCHEMA)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .withDataModel(GenericData.get())
                .build()) {

            for (GenericData.Record record : recordsToWrite) {
                writer.write(record);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
*/
