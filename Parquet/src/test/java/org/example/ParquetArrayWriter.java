//package org.example;
//
//import org.apache.avro.generic.GenericData;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.parquet.hadoop.ParquetWriter;
//import org.apache.parquet.hadoop.metadata.CompressionCodecName;
//import org.apache.parquet.schema.MessageType;
//import org.apache.parquet.schema.MessageTypeParser;
//import org.apache.parquet.hadoop.ParquetFileWriter;
//
//
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
//public class ParquetArrayWriter {
//        public static void main(String[] args) throws IOException {
//            System.setProperty("hadoop.home.dir", "C:\\Users\\MKishore\\Downloads\\Hadoop Configuration");
//            Configuration configuration = new Configuration();
//            Path file = new Path("output.parquet");
//            MessageType schema = MessageTypeParser.parseMessageType(
//                    "message mymessage {\n" +
//                            "  required group mygroup {\n" +
//                            "    repeated int32 myarray;\n" +
//                            "  }\n" +
//                            "}");
//
//            ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(file)
//                    .withSchema(schema)
//                    .withCompressionCodec(CompressionCodecName.SNAPPY)
//                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
//                    .withConf(configuration)
//                    .build();
//
//            GenericRecord record = new GenericData.Record(schema);
//            List<Integer> myarray = new ArrayList<>();
//            myarray.add(1);
//            myarray.add(2);
//            myarray.add(3);
//            record.put("mygroup", new GenericData.Record(schema.getType("mygroup")).put("myarray", myarray));
//
//            writer.write(record);
//            writer.close();
//        }
//    }
//
//}
