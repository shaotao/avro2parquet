package com.example.avro2parquet;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * parquet util class to process Parquet files.
 */
@Slf4j
public class ParquetUtil {

    private static final long MAX_COUNT_TO_SCAN = 10000000; // 10 million to scan max, otherwise too slow
    private static final long MAX_COUNT_TO_DISPLAY = 1000; // 1000 records to display max to fix in HTTP reply

    private static String headerToString(Group g) {
        StringBuffer buf = new StringBuffer();
        List<String> nameList = new ArrayList<>();
        int fieldCount = g.getType().getFieldCount();
        for (int field = 0; field < fieldCount; field++) {
            Type fieldType = g.getType().getType(field);
            String fieldName = fieldType.getName();
            nameList.add(fieldName);
        }
        buf.append(String.join(" | ", nameList));
        return buf.toString();
    }

    private static String groupToString(Group g) {
        StringBuffer buf = new StringBuffer();
        List<String> valueList = new ArrayList<>();
        int fieldCount = g.getType().getFieldCount();
        for (int field = 0; field < fieldCount; field++) {
            int valueCount = g.getFieldRepetitionCount(field);

            Type fieldType = g.getType().getType(field);
            String fieldName = fieldType.getName();
            for (int index = 0; index < valueCount; index++) {
                String value = "(complex data type hidden...)";
                if (fieldType.isPrimitive()) {
                    value = g.getValueToString(field, index);
                }
                valueList.add(String.format("%s", value));
            }
        }
        buf.append(String.join(" | ", valueList));
        return buf.toString();
    }


    public static String readParquet(String parquetFilePath, long readCount) throws IOException {
        StringBuffer buf = new StringBuffer();
        Configuration conf = new Configuration();
        Path path = new Path(parquetFilePath);

        ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
        MessageType schema = readFooter.getFileMetaData().getSchema();
        ParquetFileReader r = new ParquetFileReader(conf, path, readFooter);

        PageReadStore pages = null;
        boolean allScanned = true;
        buf.append(String.format("parquet schema = %s\n", schema));
        long recordCount = 0;
        try {
            // get the total count
            while (null != (pages = r.readNextRowGroup())) {
                final long rows = pages.getRowCount();
                recordCount += rows;
                if (recordCount >= MAX_COUNT_TO_SCAN) {
                    allScanned = false;
                    break;
                }
            }

            r.close();

            buf.append(String.format(">>> list of records, total count %s %d, # of records to read: %d >>>\n",
                    allScanned?"=":">", recordCount, readCount));

            // get the record contents
            r = new ParquetFileReader(conf, path, readFooter);
            long count = 0L;
            boolean stopReading = false;
            while (null != (pages = r.readNextRowGroup()) && !stopReading) {
                final long rows = pages.getRowCount();

                final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                final RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                for (int i = 0; i < rows; i++) {
                    count++;
                    if (count > MAX_COUNT_TO_DISPLAY || count > readCount) {
                        buf.append(String.format("(%s%d records have been skipped...)\n", allScanned?"":"more than ", recordCount-count+1));
                        stopReading = true;
                        break;
                    }
                    final Group g = recordReader.read();
                    if (i == 0) {
                        String header = headerToString(g);
                        buf.append(header+"\n");
                    }
                    String str = groupToString(g);
                    buf.append(count+") "+str+"\n");
                }
            }
        } finally {
            r.close();
        }

        buf.append(String.format("<<< list of records, total count %s %d, # of records to read: %d <<<\n",
                allScanned?"=":">", recordCount, readCount));

        return buf.toString();
    }
}
