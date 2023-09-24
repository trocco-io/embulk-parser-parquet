package org.embulk.guess.parquet;

import static org.embulk.guess.parquet.ParquetGuessUtil.convertType;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.embulk.spi.type.Types;
import org.junit.Test;

public class TestParquetGuessUtil {

    private org.embulk.spi.type.Type convertedType(Schema schema) {
        return convertType(new Schema.Field("value", schema));
    }

    @Test
    public void testConvertType() {
        assertEquals(Types.STRING, convertedType(Schema.create(Type.STRING)));
        assertEquals(Types.STRING, convertedType(Schema.create(Type.BYTES)));
        assertEquals(Types.STRING, convertedType(Schema.create(Type.NULL)));
        assertEquals(Types.LONG, convertedType(Schema.create(Type.INT)));
        assertEquals(Types.LONG, convertedType(Schema.create(Type.LONG)));
        assertEquals(Types.DOUBLE, convertedType(Schema.create(Type.FLOAT)));
        assertEquals(Types.DOUBLE, convertedType(Schema.create(Type.DOUBLE)));
        assertEquals(Types.BOOLEAN, convertedType(Schema.create(Type.BOOLEAN)));

        assertEquals(Types.STRING, convertedType(Schema.createFixed("name", "doc", "space", 0)));

        Schema enumSchema = Schema.createEnum("n", "d", "s", new ArrayList<>(Arrays.asList("v")));
        assertEquals(Types.STRING, convertedType(enumSchema));

        assertEquals(Types.JSON, convertedType(Schema.createMap(Schema.create(Type.LONG))));
        assertEquals(Types.JSON, convertedType(Schema.createArray(Schema.create(Type.LONG))));

        Schema recordSchema = Schema.createRecord("name", "doc", "namespace", false);
        assertEquals(Types.JSON, convertedType(recordSchema));

        Schema uSchema = Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.FLOAT));
        assertEquals(Types.DOUBLE, convertedType(uSchema));
    }
}
