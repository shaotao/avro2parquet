package com.example.avro2parquet;


import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 * avro util class to process Avro files
 */
@Slf4j
public class AvroUtil {

    private static final Schema.Parser schemaParser = new Schema.Parser();
    private static final long MAX_COUNT_TO_SCAN = 10000000; // 10 million to scan max, otherwise too slow
    private static final long MAX_COUNT_TO_DISPLAY = 1000; // 1000 records to display max to fix in HTTP reply

    // get DataFileReader from avro file path
    public static DataFileReader<GenericRecord> getDataFileReader(String filepath) throws IOException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File(filepath), datumReader);
        return dataFileReader;
    }

    // get avro Schema from DataFileReader
    public static Schema getSchema(DataFileReader<GenericRecord> dataFileReader) {
        return dataFileReader.getSchema();
    }

    // build a schema from a input avsc file
    public static Schema buildSchema(String avscFilePath) throws IOException {
        Schema schema = schemaParser.parse(new File(avscFilePath));
        return schema;
    }

    // read avro file
    public static String readAvro(String avroFilePath, long readCount) throws IOException {
        StringBuffer buf = new StringBuffer();
        DataFileReader<GenericRecord> reader = getDataFileReader(avroFilePath);
        Schema schema = reader.getSchema();
        buf.append(String.format(">>> avro schema of %s >>>\n", avroFilePath));
        buf.append(String.format("%s\n", schema.toString()));
        buf.append(String.format("<<< avro schema of %s <<<\n", avroFilePath));
        Iterator<GenericRecord> iter = reader.iterator();
        long recordCount = 0;
        boolean allScanned = true;
        while (iter.hasNext()) {
            recordCount++;
            iter.next();
            if (recordCount > MAX_COUNT_TO_SCAN) {
                allScanned = false;
                break;
            }
        }
        buf.append(String.format(">>> list of records, total count %s %d, # of records to read: %d >>>\n",
                allScanned?"=":">", recordCount, readCount));
        reader.seek(0);
        reader.sync(0);
        iter = reader.iterator();
        long count = 0;
        while (iter.hasNext()) {
            count++;
            if (count > MAX_COUNT_TO_DISPLAY || count > readCount) {
                buf.append(String.format("(%s%d records have been skipped...)\n", allScanned?"":"more than ", recordCount-count+1));
                break;
            }
            GenericRecord record = iter.next();
            buf.append(String.format("%d) %s\n", count, record.toString()));
        }
        buf.append(String.format("<<< list of records, total count %s %d, # of records to read: %d <<<\n",
                allScanned?"=":">", recordCount, readCount));
        reader.close();
        return buf.toString();
    }
}
