package org.embulk.parser.parquet.getter;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

public class IntegerColumnGetter extends BaseColumnGetter {
    protected Integer value;

    private final TimestampUnit timestampUnit;

    public IntegerColumnGetter(PageBuilder pageBuilder, TimestampUnit timestampUnit) {
        super(pageBuilder);
        this.timestampUnit = timestampUnit;
    }

    @Override
    public void setValue(Object value)
    {
        this.value = (Integer) value;
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
            pageBuilder.setTimestamp(column, timestampUnit.toTimestamp(value.longValue()));
        }
    }
}
