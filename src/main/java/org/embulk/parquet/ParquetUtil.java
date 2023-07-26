package org.embulk.parquet;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class ParquetUtil {
    private static final byte[] PARQUET_HEADER = {0x50, 0x41, 0x52, 0x31};
    static public boolean isParquetFile(byte[] bytes) {
        if(PARQUET_HEADER.length > bytes.length) {
            return false;
        }
        for(int i = 0; i < PARQUET_HEADER.length; i++) {
            if(PARQUET_HEADER[i] != bytes[i]) {
                return false;
            }
        }
        return true;
    }
    static private ParquetReader<Object> buildReader(InputStream inputStream) {
        // FIXME: AvroParquetReader.builder takes only a path argument, find a stream argument
        final String path = copyToFile(inputStream).getAbsolutePath();
        Configuration conf = new Configuration();
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        try {
            return AvroParquetReader.builder(new Path(path))
                        .withConf(conf)
                        .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static public GenericRecord fetchRecord(InputStream inputStream) {
        ParquetReader<Object> reader = buildReader(inputStream);
        try {
            while(true) {
                final Object obj = reader.read();
                if(obj == null) {
                    return null;
                }
                if (obj instanceof GenericRecord) {
                    return ((GenericRecord) obj);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static public List<GenericRecord> fetchRecords(InputStream inputStream) {
        final ParquetReader<Object> reader = buildReader(inputStream);
        List<GenericRecord> records = new ArrayList<>();
        try {
            while(true) {
                final Object obj = reader.read();
                if(obj == null) {
                    break;
                }
                if (obj instanceof GenericRecord) {
                    records.add(((GenericRecord) obj));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return records;
    }

    static private File copyToFile(InputStream input) {
        final File tmpfile;
        try {
            tmpfile = Files.createTempFile("embulk-input-parquet.", ".tmp").toFile();
            tmpfile.deleteOnExit();
            try (FileOutputStream output = new FileOutputStream(tmpfile)) {
                IOUtils.copy(input, output);
            } finally {
                input.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return tmpfile;
    }
}
