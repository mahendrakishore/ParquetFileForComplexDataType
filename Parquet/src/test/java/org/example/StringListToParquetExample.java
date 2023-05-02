//package org.example;
//
//import java.io.IOException;
//import java.util.List;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.parquet.column.ParquetProperties.WriterVersion;
//import org.apache.parquet.example.data.Group;
//import org.apache.parquet.example.data.simple.SimpleGroupFactory;
//import org.apache.parquet.hadoop.ParquetWriter;
//import org.apache.parquet.hadoop.example.GroupWriteSupport;
//import org.apache.parquet.hadoop.metadata.CompressionCodecName;
//import org.apache.parquet.schema.MessageType;
//import org.apache.parquet.schema.MessageTypeParser;
//import org.apache.parquet.Version;
//public class StringListToParquetExample {
//
//    public static void main(String[] args) throws IOException {
//        Configuration conf = new Configuration();
//        System.setProperty("hadoop.home.dir", "C:\\Users\\MKishore\\Downloads\\Hadoop Configuration");
//        FileSystem fs = FileSystem.get(conf);
//        Path outputPath = new Path("output.parquet");
//        MessageType schema = MessageTypeParser.parseMessageType(
//                "message schema {\n" +
//                        "  required array array_field;\n" +
//                        "}");
//
//        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
//        GroupWriteSupport.setSchema(schema, conf);
//        System.out.println(Version.VERSION_NUMBER);
//        ParquetWriter<Group> writer = new ParquetWriter<Group>(
//                outputPath,
//                new GroupWriteSupport(),
//                CompressionCodecName.SNAPPY,
//                128 * 1024 * 1024,
//                1 * 1024 * 1024,
//                4 * 1024 * 1024,
//                true,
//                false,
//                WriterVersion.PARQUET_2_0,
//                conf);
//
//        for (double str : getListOfStrings()) {
//            Group group = groupFactory.newGroup().append("double_field", str);
//            writer.write(group);
//        }
//
//        writer.close();
//    }
//
//    private static List<int[]> getListOfStrings() {
//        // Replace this method with your own implementation that returns a List<String>
//        return List.of([1,2,3]);
//    }
//}
