package org.embulk.guess.parquet;

import org.apache.avro.Schema;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

import java.util.*;
import java.util.stream.Collectors;

public class ParquetGuessUtil {

    static public List<Map<String, String>> createColumns(List<Schema.Field> fields) {
        return fields.stream().map(field -> {
            Map<String, String> column = new HashMap<>();
            column.put("name", field.name());
            column.put("type", convertType(field).getName());
            return  column;
        }).collect(Collectors.toList());
    }

    private static final Map<Schema.Type, Type> TYPE_MAP = new EnumMap<>(Schema.Type.class);

    static {
        TYPE_MAP.put(Schema.Type.STRING, Types.STRING);
        TYPE_MAP.put(Schema.Type.BYTES, Types.STRING);
        TYPE_MAP.put(Schema.Type.FIXED, Types.STRING);
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

    static public Type convertType(Schema.Field field) {
        Schema.Type type = field.schema().getType();
        if(type != Schema.Type.UNION) {
            return TYPE_MAP.get(type);
        }

        Schema.Type firstNotNullTypeInTypes = field.schema().getTypes().stream().map(s -> s.getType()).filter(t -> t != Schema.Type.NULL).findFirst().orElse(null);
        return TYPE_MAP.get(firstNotNullTypeInTypes);
    }
}
