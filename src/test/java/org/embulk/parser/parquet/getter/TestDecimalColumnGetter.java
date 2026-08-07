package org.embulk.parser.parquet.getter;

import static org.junit.Assert.assertThrows;

import java.nio.ByteBuffer;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.type.Types;
import org.junit.Test;

public class TestDecimalColumnGetter {
    @Test
    public void testEmptyByteArrayRaisesDataException() {
        final DecimalColumnGetter getter = new DecimalColumnGetter(null, 0);
        assertThrows(DataException.class, () -> getter.setValue(ByteBuffer.wrap(new byte[] {})));
    }

    @Test
    public void testUnsupportedColumnsRaiseDataException() {
        final DecimalColumnGetter getter = new DecimalColumnGetter(null, 0);
        getter.setValue(ByteBuffer.wrap(new byte[] {0x7b}));

        assertThrows(DataException.class, () -> getter.booleanColumn(column(Types.BOOLEAN)));
        assertThrows(DataException.class, () -> getter.timestampColumn(column(Types.TIMESTAMP)));
        assertThrows(DataException.class, () -> getter.jsonColumn(column(Types.JSON)));
    }

    private Column column(org.embulk.spi.type.Type type) {
        return new Column(0, "value", type);
    }
}
