package org.embulk.parser.parquet.getter;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.type.Types;
import org.junit.Test;

public class TestBytesColumnGetter {
    @Test
    public void testToHex() {
        assertEquals("", BytesColumnGetter.toHex(new byte[] {}));
        assertEquals("00", BytesColumnGetter.toHex(new byte[] {0x00}));
        assertEquals("0f", BytesColumnGetter.toHex(new byte[] {0x0f}));
        // Signed bytes must not be printed as a negative number.
        assertEquals("ff", BytesColumnGetter.toHex(new byte[] {(byte) 0xff}));
        assertEquals("0102fe", BytesColumnGetter.toHex(new byte[] {0x01, 0x02, (byte) 0xfe}));
    }

    @Test
    public void testToBytesFromByteBuffer() {
        final ByteBuffer buffer = ByteBuffer.wrap(new byte[] {0x01, 0x02});
        assertArrayEquals(new byte[] {0x01, 0x02}, BytesColumnGetter.toBytes(buffer));
        // The original buffer must not be consumed, so the same value can be read again.
        assertEquals(2, buffer.remaining());
        assertArrayEquals(new byte[] {0x01, 0x02}, BytesColumnGetter.toBytes(buffer));
    }

    @Test
    public void testToBytesFromGenericFixed() {
        final Schema schema = Schema.createFixed("fixed", "doc", "space", 2);
        final GenericData.Fixed fixed = new GenericData.Fixed(schema, new byte[] {0x0a, 0x0b});
        assertEquals("0a0b", BytesColumnGetter.toHex(BytesColumnGetter.toBytes(fixed)));
    }

    @Test
    public void testToBytesFromByteArray() {
        assertArrayEquals(new byte[] {0x01}, BytesColumnGetter.toBytes(new byte[] {0x01}));
    }

    @Test
    public void testToBytesFromUnsupportedValue() {
        assertThrows(DataException.class, () -> BytesColumnGetter.toBytes("not bytes"));
    }

    @Test
    public void testNonStringColumnsRaiseDataException() {
        // Reading a hexadecimal string as a number or a timestamp would be silently wrong.
        // These must raise a DataException rather than a NullPointerException or a wrong value.
        final BytesColumnGetter getter = new BytesColumnGetter(null);
        getter.setValue(ByteBuffer.wrap(new byte[] {0x01, 0x02}));

        assertThrows(DataException.class, () -> getter.longColumn(column(Types.LONG)));
        assertThrows(DataException.class, () -> getter.doubleColumn(column(Types.DOUBLE)));
        assertThrows(DataException.class, () -> getter.booleanColumn(column(Types.BOOLEAN)));
        assertThrows(DataException.class, () -> getter.timestampColumn(column(Types.TIMESTAMP)));
        assertThrows(DataException.class, () -> getter.jsonColumn(column(Types.JSON)));
    }

    private Column column(org.embulk.spi.type.Type type) {
        return new Column(0, "value", type);
    }
}
