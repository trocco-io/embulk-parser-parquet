package org.embulk.parquet;

import java.io.*;
import java.nio.file.Files;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

public class ParquetUtil {
    private static final byte[] PARQUET_MAGIC_NUMBER = {0x50, 0x41, 0x52, 0x31};

    public static boolean isParquetFile(byte[] bytes) {
        if (PARQUET_MAGIC_NUMBER.length * 2 > bytes.length) {
            return false;
        }
        for (int i = 0; i < PARQUET_MAGIC_NUMBER.length; i++) {
            byte magicNumber = PARQUET_MAGIC_NUMBER[i];
            int lastIndex = bytes.length - PARQUET_MAGIC_NUMBER.length + i;
            if (magicNumber != bytes[i] || magicNumber != bytes[lastIndex]) {
                return false;
            }
        }
        return true;
    }

    public static ParquetReader<Object> buildReader(InputStream inputStream) {
        final String path = copyToFile(inputStream).getAbsolutePath();
        AvroParquetReader.Builder<Object> builder = AvroParquetReader.builder(new Path(path));
        Configuration configuration = new Configuration();
        configuration.set("fs.file.impl", LocalFileSystem.class.getName());
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
