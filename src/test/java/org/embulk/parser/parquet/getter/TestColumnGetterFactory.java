package org.embulk.parser.parquet.getter;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.type.Types;
import org.embulk.util.timestamp.TimestampFormatter;
import org.junit.Test;

public class TestColumnGetterFactory {
    @Test
    public void testNewColumnGetter() {
        assertTrue(newColumnGetter(Schema.create(Type.STRING)) instanceof StringColumnGetter);
        assertTrue(newColumnGetter(Schema.create(Type.INT)) instanceof IntegerColumnGetter);
        assertTrue(newColumnGetter(Schema.create(Type.LONG)) instanceof LongColumnGetter);
        assertTrue(newColumnGetter(Schema.create(Type.FLOAT)) instanceof FloatColumnGetter);
        assertTrue(newColumnGetter(Schema.create(Type.DOUBLE)) instanceof DoubleColumnGetter);
        assertTrue(newColumnGetter(Schema.create(Type.BOOLEAN)) instanceof BooleanColumnGetter);
        assertTrue(newColumnGetter(Schema.create(Type.NULL)) instanceof StringColumnGetter);

        Schema enumSchema = Schema.createEnum("n", "d", "n", new ArrayList<>());
        assertTrue(newColumnGetter(enumSchema) instanceof StringColumnGetter);

        Schema mapSchema = Schema.createMap(Schema.create(Type.LONG));
        assertTrue(newColumnGetter(mapSchema) instanceof GenericDataColumnGetter);

        Schema arraySchema = Schema.createArray(Schema.create(Type.LONG));
        assertTrue(newColumnGetter(arraySchema) instanceof GenericDataColumnGetter);

        Schema recordSchema = Schema.createRecord("name", "doc", "namespace", false);
        assertTrue(newColumnGetter(recordSchema) instanceof GenericDataColumnGetter);

        Schema uSchema = Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.FLOAT));
        assertTrue(newColumnGetter(uSchema) instanceof FloatColumnGetter);

        assertTrue(newColumnGetter(Schema.create(Type.BYTES)) instanceof BytesColumnGetter);

        Schema fixedSchema = Schema.createFixed("name", "doc", "space", 4);
        assertTrue(newColumnGetter(fixedSchema) instanceof BytesColumnGetter);

        Schema nullableBytes =
                Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.BYTES));
        assertTrue(newColumnGetter(nullableBytes) instanceof BytesColumnGetter);
    }

    @Test
    public void testNewColumnGetterForDecimal() {
        Schema decimalBytes = LogicalTypes.decimal(20, 0).addToSchema(Schema.create(Type.BYTES));
        assertTrue(newColumnGetter(decimalBytes) instanceof DecimalColumnGetter);

        Schema decimalFixed =
                LogicalTypes.decimal(20, 0)
                        .addToSchema(Schema.createFixed("decimal", "doc", "space", 9));
        assertTrue(newColumnGetter(decimalFixed) instanceof DecimalColumnGetter);

        // The logical type has to survive the union unwrapping, otherwise a nullable BIGINT
        // UNSIGNED would be read as the hexadecimal of its two's complement.
        Schema nullableDecimal =
                Schema.createUnion(
                        Schema.create(Type.NULL),
                        LogicalTypes.decimal(20, 0)
                                .addToSchema(Schema.createFixed("nd", "doc", "space", 9)));
        assertTrue(newColumnGetter(nullableDecimal) instanceof DecimalColumnGetter);
    }

    @Test
    public void testNewColumnGetterForUnknownColumn() {
        Schema schema = Schema.createRecord("record", "doc", "namespace", false, new ArrayList<>());
        ColumnGetterFactory factory =
                new ColumnGetterFactory(
                        schema, null, new TimestampFormatter[] {null}, new TimestampUnit[] {null});
        assertThrows(
                DataException.class,
                () -> factory.newColumnGetter(new Column(0, "missing", Types.STRING)));
    }

    private BaseColumnGetter newColumnGetter(Schema fieldSchema) {
        TimestampUnit[] timestampUnits = new TimestampUnit[] {TimestampUnit.Second};
        TimestampFormatter[] timestampFormatters =
                new TimestampFormatter[] {
                    TimestampFormatter.builder("yyyy", true).setDefaultZoneFromString("UTC").build()
                };
        List<Schema.Field> fields =
                new ArrayList<>(Arrays.asList(new Schema.Field("value", fieldSchema)));
        Schema schema = Schema.createRecord("record", "doc", "namespace", false, fields);
        ColumnGetterFactory columnGetterFactory =
                new ColumnGetterFactory(schema, null, timestampFormatters, timestampUnits);
        return columnGetterFactory.newColumnGetter(new Column(0, "value", Types.STRING));
    }
}
