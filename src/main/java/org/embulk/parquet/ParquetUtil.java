package org.embulk.parquet;

import java.io.*;
import java.nio.file.Files;
import java.util.EnumMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.embulk.spi.DataException;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

public class ParquetUtil {
    private static final byte[] PARQUET_MAGIC_NUMBER = {0x50, 0x41, 0x52, 0x31};

    private static final Map<Schema.Type, Type> TYPE_MAP = new EnumMap<>(Schema.Type.class);

    static {
        TYPE_MAP.put(Schema.Type.STRING, Types.STRING);
        TYPE_MAP.put(Schema.Type.ENUM, Types.STRING);
        TYPE_MAP.put(Schema.Type.NULL, Types.STRING);
        TYPE_MAP.put(Schema.Type.INT, Types.LONG);
        TYPE_MAP.put(Schema.Type.LONG, Types.LONG);
        TYPE_MAP.put(Schema.Type.FLOAT, Types.DOUBLE);
        TYPE_MAP.put(Schema.Type.DOUBLE, Types.DOUBLE);
        TYPE_MAP.put(Schema.Type.BOOLEAN, Types.BOOLEAN);
        TYPE_MAP.put(Schema.Type.MAP, Types.JSON);
        TYPE_MAP.put(Schema.Type.ARRAY, Types.JSON);
        TYPE_MAP.put(Schema.Type.RECORD, Types.JSON);
    }

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

    public static Type convertType(Schema schema) {
        return TYPE_MAP.get(fetchType(schema));
    }

    public static Schema.Type fetchType(Schema schema) {
        final Schema.Type type =
                schema.getType() == Schema.Type.UNION
                        ? schema.getTypes().stream()
                                .map(s -> s.getType())
                                .filter(t -> t != Schema.Type.NULL)
                                .findFirst()
                                .orElse(null)
                        : schema.getType();
        if (!TYPE_MAP.containsKey(type)) {
            throw new DataException(String.format("%s is not supported", type.getName()));
        }
        return type;
    }
}
