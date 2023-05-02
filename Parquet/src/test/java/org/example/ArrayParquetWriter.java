/*
package org.example;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

public class ArrayParquetWriter {

    public static void main(String[] args) throws IOException {
        System.setProperty("hadoop.home.dir", "C:\\Users\\MKishore\\Downloads\\Hadoop Configuration");
        Configuration conf = new Configuration();
        MessageType schema = Types.buildMessage()
                .addField(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "array", Type.Repetition.REPEATED))
                .named("Array");
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        writeSupport.setSchema(schema, conf);
        Path outputPath = new Path("array.parquet");
        ParquetWriter<Group> writer = new ParquetWriter<>(outputPath, writeSupport, CompressionCodecName.SNAPPY, 1024, 1024, 512, true, false, ParquetWriter.DEFAULT_WRITER_VERSION, conf);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        Group group = groupFactory.newGroup();
        int[] array = new int[] {1, 2, 3, 4, 5};
        group.add("array", array);
        writer.write(group);
        writer.close();
    }
}
*/
