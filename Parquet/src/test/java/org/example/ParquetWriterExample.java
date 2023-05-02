package org.example;
import java.io.IOException;
import java.time.Instant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
public class ParquetWriterExample {

    public static void main(String[] args) throws IOException {
            Configuration conf = new Configuration();
            System.setProperty("hadoop.home.dir", "C:\\Users\\MKishore\\Downloads\\Hadoop Configuration");
            FileSystem fs = FileSystem.get(conf);
            Path outputPath = new Path("outputs1.parquet");
            MessageType schema = MessageTypeParser.parseMessageType(
                    "message schema {\n" +
                            "  required boolean bool_field;\n" +
                            "  required int32 int_field;\n" +
                            "  required int64 long_field;\n" +
                            "  required float float_field;\n" +
                            "  required double double_field;\n" +
                            "  required binary string_field;\n" +
                            "}");

            SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
            GroupWriteSupport.setSchema(schema, conf);
            ParquetWriter<Group> writer = new ParquetWriter<Group>(
                    outputPath,
                    new GroupWriteSupport(),
                    CompressionCodecName.SNAPPY,
                    128 * 1024 * 1024,
                    1 * 1024 * 1024,
                    4 * 1024 * 1024,
                    true,
                    false,
                    WriterVersion.PARQUET_2_0,
                    conf);

            Group group = groupFactory.newGroup()
                    .append("bool_field", true)
                    .append("int_field", 123)
                    .append("long_field", 456L)
                    .append("float_field", 1.23f)
                    .append("double_field", 4.56)
                    .append("string_field", "hello world");

            writer.write(group);
            writer.close();
        }
}
