package org.embulk.parser.parquet.getter;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

public class FloatColumnGetter extends BaseColumnGetter {
    protected Float value;

    private final TimestampUnit timestampUnit;

    public FloatColumnGetter(PageBuilder pageBuilder, TimestampUnit timestampUnit) {
        super(pageBuilder);
        this.timestampUnit = timestampUnit;
    }

    @Override
    public void setValue(Object value)
    {
        this.value = (Float) value;
    }

    @Override
    public void longColumn(Column column) {
        if (value == null) {
            pageBuilder.setNull(column);
        } else {
            pageBuilder.setLong(column, value.longValue());
        }
    }

    @Override
    public void doubleColumn(Column column) {
        if (value == null) {
            pageBuilder.setNull(column);
        } else {
            pageBuilder.setDouble(column, value.doubleValue());
        }
    }

    @Override
    public void stringColumn(Column column) {
        if (value == null) {
            pageBuilder.setNull(column);
        } else {
            pageBuilder.setString(column, value.toString());
        }
    }

    @Override
    public void timestampColumn(Column column) {
        if (this.value == null) {
            pageBuilder.setNull(column);
        }
        else {
            pageBuilder.setTimestamp(column, timestampUnit.toTimestamp(value.doubleValue()));
        }
    }
}
