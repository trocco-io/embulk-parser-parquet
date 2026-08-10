package org.embulk.parser.parquet.getter;

import java.nio.ByteBuffer;
import org.apache.avro.generic.GenericFixed;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.PageBuilder;

/**
 * Reads Avro BYTES / FIXED, that is Parquet BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY without a string
 * annotation, as a lowercase hexadecimal string.
 *
 * <p>Only string columns are supported on purpose. A hexadecimal string parsed as a number or a
 * timestamp would be silently wrong, so the other column types fall back to {@link
 * BaseColumnGetter} and raise a DataException.
 */
public class BytesColumnGetter extends BaseColumnGetter {
    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

    private String hexValue;

    public BytesColumnGetter(PageBuilder pageBuilder) {
        super(pageBuilder);
    }

    @Override
    public void setValue(Object value) {
        super.setValue(value);
        this.hexValue = value == null ? null : toHex(toBytes(value));
    }

    @Override
    public void stringColumn(Column column) {
        if (hexValue == null) {
            pageBuilder.setNull(column);
        } else {
            pageBuilder.setString(column, hexValue);
        }
    }

    public static byte[] toBytes(Object value) {
        if (value instanceof ByteBuffer) {
            // duplicate() so that reading the value does not consume the original buffer.
            final ByteBuffer buffer = ((ByteBuffer) value).duplicate();
            final byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return bytes;
        }
        if (value instanceof GenericFixed) {
            return ((GenericFixed) value).bytes();
        }
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        throw new DataException(
                String.format("cannot convert value from %s to bytes", value.getClass()));
    }

    public static String toHex(byte[] bytes) {
        final char[] chars = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            final int b = bytes[i] & 0xff;
            chars[i * 2] = HEX_CHARS[b >>> 4];
            chars[i * 2 + 1] = HEX_CHARS[b & 0x0f];
        }
        return new String(chars);
    }
}
