package org.embulk.parser.parquet.getter;

import static org.embulk.parser.parquet.getter.ParquetGenericDataConverter.convert;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.junit.Test;
import org.msgpack.value.*;
import org.msgpack.value.impl.*;

public class TestParquetGenericDataConverter {
    @Test
    public void testConvertUTF8() {
        assertEquals(
                new ImmutableStringValueImpl("test"), convert(new Utf8("test")).asStringValue());
    }

    @Test
    public void testConvertInteger() {
        assertEquals(new ImmutableLongValueImpl(1L), convert(new Integer(1)).asIntegerValue());
    }

    @Test
    public void testConvertLong() {
        assertEquals(new ImmutableLongValueImpl(1L), convert(new Long(1)).asIntegerValue());
    }

    @Test
    public void testConvertFloat() {
        assertEquals(new ImmutableDoubleValueImpl(1.0f), convert(new Float(1)).asFloatValue());
    }

    @Test
    public void testConvertDouble() {
        assertEquals(new ImmutableDoubleValueImpl(1.0f), convert(new Double(1)).asFloatValue());
    }

    @Test
    public void testConvertBoolean() {
        assertEquals(ImmutableBooleanValueImpl.TRUE, convert(new Boolean(true)).asBooleanValue());
    }

    @Test
    public void testConvertEnumSymbol() {
        Schema schema =
                SchemaBuilder.record("Record1")
                        .fields()
                        .name("field1")
                        .type("string")
                        .noDefault()
                        .endRecord();
        GenericData.EnumSymbol enumSymbol = new GenericData.EnumSymbol(schema, "test");
        assertEquals(new ImmutableStringValueImpl("test"), convert(enumSymbol).asStringValue());
    }

    @Test
    public void testConvertGenericDataArray() {
        Schema schema = Schema.createArray(Schema.create(Schema.Type.LONG));
        GenericData.Array<Long> list = new GenericData.Array<Long>(1, schema);
        list.add(1L);
        List<Value> result = convert(list).asArrayValue().list();
        assertEquals(1, result.size());
        assertEquals(new ImmutableLongValueImpl(1L), result.get(0));
    }

    @Test
    public void testConvertArrayList() {
        List<Long> list = new ArrayList<>();
        list.add(1L);
        List<Value> result = convert(list).asArrayValue().list();
        assertEquals(1, result.size());
        assertEquals(new ImmutableLongValueImpl(1L), result.get(0));
    }

    @Test
    public void testConvertGenericRecord() {
        List<Schema.Field> fields = new ArrayList<>();
        fields.add(new Schema.Field("value", Schema.create(Schema.Type.LONG)));
        Schema schema = Schema.createRecord("record", "doc", "namespace", false, fields);
        GenericData.Record record = new GenericData.Record(schema);
        record.put("value", 1L);
        Map<Value, Value> result = convert(record).asMapValue().map();
        assertEquals(1, result.size());
        assertEquals(
                new ImmutableLongValueImpl(1L), result.get(new ImmutableStringValueImpl("value")));
    }

    @Test
    public void testConvertHashMap() {
        HashMap<Utf8, Long> hashMap = new HashMap<>();
        hashMap.put(new Utf8("value"), 1L);
        Map<Value, Value> result = convert(hashMap).asMapValue().map();
        assertEquals(1, result.size());
        assertEquals(
                new ImmutableLongValueImpl(1L), result.get(new ImmutableStringValueImpl("value")));
    }

    @Test
    public void testConvertNull() {
        assertEquals(ImmutableNilValueImpl.get(), convert(null).asNilValue());
    }
}
