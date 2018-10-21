package com.example.avro2parquet;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Iterator;

/**
 * File format util class
 */
@Slf4j
public class FileFormatUtil {

    private static final AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter();

    public static void convertAvroToParquet(String avroFilePath, String parquetFilePath) throws IOException {
        DataFileReader<GenericRecord> dataFileReader = null;
        AvroParquetWriter avroParquetWriter = null;

        try {
            // get avro DataFileReader
            dataFileReader = AvroUtil.getDataFileReader(avroFilePath);
            Schema avroSchema = dataFileReader.getSchema();
            log.info(String.format("avro schema = %s", avroSchema));

            CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;
            int blockSize = 256 * 1024 * 1024;
            int pageSize = 64 * 1024;

            Path outputPath = new Path(parquetFilePath);

            //MessageType parquetSchema = new AvroSchemaConverter().convert(avroSchema);
            //AvroWriteSupport writeSupport = new AvroWriteSupport(parquetSchema, avroSchema);
            // the ParquetWriter object that will consume Avro GenericRecords
            //ParquetWriter parquetWriter = new ParquetWriter(outputPath,
            //        writeSupport, compressionCodecName, blockSize, pageSize);

            avroParquetWriter = new AvroParquetWriter(outputPath, avroSchema, compressionCodecName, blockSize, pageSize);

            Iterator<GenericRecord> iterator = dataFileReader.iterator();
            while (iterator.hasNext()) {
                GenericRecord record = iterator.next();
                avroParquetWriter.write(record);
            }
        } finally {
            if (dataFileReader != null) {
                try {
                    dataFileReader.close();
                } catch (Exception e) {
                    log.error("Failed to close DataFileReader for avro!", e);
                }
            }

            if (avroParquetWriter != null) {
                try {
                    avroParquetWriter.close();
                } catch (Exception e) {
                    log.error("Failed to close AvroParquetWriter!", e);
                }
            }
        }
    }
}
