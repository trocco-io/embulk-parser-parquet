package org.embulk.parquet;

import java.io.*;
import java.nio.file.Files;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

public class ParquetUtil {
    private static final byte[] PARQUET_MAGIC_NUMBER = {0x50, 0x41, 0x52, 0x31};

    public static boolean isParquetFile(byte[] bytes) {
        if (PARQUET_MAGIC_NUMBER.length > bytes.length) {
            return false;
        }
        for (int i = 0; i < PARQUET_MAGIC_NUMBER.length; i++) {
            if (PARQUET_MAGIC_NUMBER[i] != bytes[i]
                    || PARQUET_MAGIC_NUMBER[i]
                            != bytes[bytes.length - PARQUET_MAGIC_NUMBER.length + i]) {
                return false;
            }
        }
        return true;
    }

    public static ParquetReader<Object> buildReader(InputStream inputStream) {
        final String path = copyToFile(inputStream).getAbsolutePath();
        AvroParquetReader.Builder<Object> builder = AvroParquetReader.builder(new Path(path));
        org.apache.hadoop.conf.Configuration configuration =
                new org.apache.hadoop.conf.Configuration();
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        try {
            return builder.withConf(configuration).build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static File copyToFile(InputStream input) {
        try {
            final File tmpfile = Files.createTempFile("embulk-input-parquet.", ".tmp").toFile();
            tmpfile.deleteOnExit();
            try (FileOutputStream output = new FileOutputStream(tmpfile)) {
                IOUtils.copy(input, output);
            }
            return tmpfile;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
