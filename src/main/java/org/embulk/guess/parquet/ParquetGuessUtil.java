package org.embulk.guess.parquet;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;
import org.embulk.parquet.ParquetUtil;

public class ParquetGuessUtil {
    public static GenericRecord fetchRecord(InputStream inputStream) {
        final ParquetReader<Object> reader = ParquetUtil.buildReader(inputStream);
        try {
            while (true) {
                final Object obj = reader.read();
                if (obj == null) {
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

    public static List<Map<String, String>> createColumns(List<Schema.Field> fields) {
        return fields.stream()
                .map(
                        field -> {
                            Map<String, String> column = new HashMap<>();
                            column.put("name", field.name());
                            column.put("type", ParquetUtil.convertType(field.schema()).getName());
                            return column;
                        })
                .collect(Collectors.toList());
    }
}
