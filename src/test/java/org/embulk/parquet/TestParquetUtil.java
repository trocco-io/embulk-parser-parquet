package org.embulk.parquet;

import static org.embulk.parquet.ParquetUtil.isParquetFile;
import static org.junit.Assert.*;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.embulk.spi.DataException;
import org.embulk.spi.type.Types;
import org.junit.Test;

public class TestParquetUtil {
    @Test
    public void testIsParquet() {
        assertTrue(isParquetFile(new byte[] {0x50, 0x41, 0x52, 0x31, 0x50, 0x41, 0x52, 0x31}));
        assertTrue(isParquetFile(new byte[] {0x50, 0x41, 0x52, 0x31, 0, 0x50, 0x41, 0x52, 0x31}));
        assertFalse(isParquetFile(new byte[] {0x50, 0x41, 0x52, 0, 0, 0x50, 0x41, 0x52, 0x31}));
        assertFalse(isParquetFile(new byte[] {0x50, 0x41, 0x52, 0x31}));
    }

    @Test
    public void testConvertType() {
        assertEquals(Types.STRING, convertedType(Schema.create(Type.STRING)));
        assertEquals(Types.STRING, convertedType(Schema.create(Type.NULL)));
        assertEquals(Types.LONG, convertedType(Schema.create(Type.INT)));
        assertEquals(Types.LONG, convertedType(Schema.create(Type.LONG)));
        assertEquals(Types.DOUBLE, convertedType(Schema.create(Type.FLOAT)));
        assertEquals(Types.DOUBLE, convertedType(Schema.create(Type.DOUBLE)));
        assertEquals(Types.BOOLEAN, convertedType(Schema.create(Type.BOOLEAN)));

        Schema enumSchema = Schema.createEnum("n", "d", "s", new ArrayList<>(Arrays.asList("v")));
        assertEquals(Types.STRING, convertedType(enumSchema));

        assertEquals(Types.JSON, convertedType(Schema.createMap(Schema.create(Type.LONG))));
        assertEquals(Types.JSON, convertedType(Schema.createArray(Schema.create(Type.LONG))));

        Schema recordSchema = Schema.createRecord("name", "doc", "namespace", false);
        assertEquals(Types.JSON, convertedType(recordSchema));

        assertEquals(Types.DOUBLE, convertedType(unionSchema(Type.NULL, Type.FLOAT)));

        assertThrows(DataException.class, () -> convertedType(Schema.create(Type.BYTES)));

        Schema fixedSchema = Schema.createFixed("name", "doc", "space", 0);
        assertThrows(DataException.class, () -> convertedType(fixedSchema));

        assertThrows(DataException.class, () -> convertedType(unionSchema(Type.NULL, Type.BYTES)));
    }

    private Schema unionSchema(Type... types) {
        return Schema.createUnion(
                Arrays.stream(types).map((x) -> Schema.create(x)).toArray(Schema[]::new));
    }

    private org.embulk.spi.type.Type convertedType(Schema schema) {
        return ParquetUtil.convertType(new Schema.Field("value", schema).schema());
    }
}
