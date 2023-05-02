//package org.example;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.parquet.hadoop.ParquetWriter;
//import org.apache.parquet.hadoop.api.WriteSupport;
//import org.apache.parquet.hadoop.metadata.CompressionCodecName;
//import org.apache.parquet.hadoop.util.HadoopOutputFile;
//import org.apache.parquet.schema.MessageType;
//import org.apache.parquet.schema.PrimitiveType;
//import org.apache.parquet.schema.Type;
//import org.apache.parquet.schema.Types;
//import org.apache.parquet.schema.Type.Repetition;
//
//public class ParquetWriterExampleA {
//
//    public static void main(String[] args) throws IOException {
//        // Define the schema for the Parquet file
//        System.setProperty("hadoop.home.dir", "C:\\Users\\MKishore\\Downloads\\Hadoop Configuration");
//        Type messageArrayType = Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE,Repetition.REPEATED).named("messageArray");
//        MessageType schema = Types.buildMessage().addFields(messageArrayType).named("root");
//
//        // Define the path for the output file
//        Path path = new Path("output.parquet");
//
//        // Define the configuration for the Hadoop cluster
//        Configuration conf = new Configuration();
//
//        // Define the compression codec to use
//        CompressionCodecName compressionCodec = CompressionCodecName.SNAPPY;
//
//        // Create the Parquet writer
//        ParquetWriter<List<Double>> writer = new ParquetWriter<List<Double>>(new Path(HadoopOutputFile.fromPath(path, conf).getPath()), WriteSupport, compressionCodec, ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE);
//
//        // Define the data to write
//        List<Double> messageArray = new ArrayList<>();
//        messageArray.add(1.0);
//        messageArray.add(2.0);
//        messageArray.add(3.0);
//
//        // Write the data to the Parquet file
//        writer.write(messageArray);
//
//        // Close the Parquet writer
//        writer.close();
//    }
//
//}
//
