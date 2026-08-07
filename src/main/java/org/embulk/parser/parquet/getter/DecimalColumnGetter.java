package org.embulk.parser.parquet.getter;

import java.math.BigDecimal;
import java.math.BigInteger;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.PageBuilder;

/**
 * Reads Avro BYTES / FIXED annotated with the decimal logical type as a decimal number.
 *
 * <p>Parquet stores such a value as a two's complement big-endian byte array, so reading it as a
 * hexadecimal string ({@link BytesColumnGetter}) would silently produce a meaningless value. MySQL
 * BIGINT UNSIGNED columns always take this path when exported to Parquet by the RDS snapshot
 * export, which maps them to FIXED_LEN_BYTE_ARRAY(9) annotated with DECIMAL(20,0).
 */
public class DecimalColumnGetter extends BaseColumnGetter {
    private final int scale;

    private BigDecimal decimalValue;

    public DecimalColumnGetter(PageBuilder pageBuilder, int scale) {
        super(pageBuilder);
        this.scale = scale;
    }

    @Override
    public void setValue(Object value) {
        super.setValue(value);
        this.decimalValue = value == null ? null : toDecimal(value);
    }

    @Override
    public void stringColumn(Column column) {
        if (decimalValue == null) {
            pageBuilder.setNull(column);
        } else {
            pageBuilder.setString(column, decimalValue.toPlainString());
        }
    }

    @Override
    public void doubleColumn(Column column) {
        if (decimalValue == null) {
            pageBuilder.setNull(column);
        } else {
            pageBuilder.setDouble(column, decimalValue.doubleValue());
        }
    }

    @Override
    public void longColumn(Column column) {
        if (decimalValue == null) {
            pageBuilder.setNull(column);
            return;
        }
        try {
            pageBuilder.setLong(column, decimalValue.longValueExact());
        } catch (ArithmeticException e) {
            throw new DataException(
                    String.format("cannot convert %s to long", decimalValue.toPlainString()), e);
        }
    }

    private BigDecimal toDecimal(Object value) {
        final byte[] bytes = BytesColumnGetter.toBytes(value);
        if (bytes.length == 0) {
            throw new DataException("cannot convert an empty byte array to a decimal");
        }
        return new BigDecimal(new BigInteger(bytes), scale);
    }
}
