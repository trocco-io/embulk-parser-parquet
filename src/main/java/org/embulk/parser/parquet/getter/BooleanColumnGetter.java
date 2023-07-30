package org.embulk.parser.parquet.getter;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

public class BooleanColumnGetter extends BaseColumnGetter {
    protected Boolean value;

    public BooleanColumnGetter(PageBuilder pageBuilder) {
        super(pageBuilder);
    }

    @Override
    public void setValue(Object value)
    {
        this.value = (Boolean) value;
    }

    @Override
    public void booleanColumn(Column column) {
        if (value == null) {
            pageBuilder.setNull(column);
        } else {
            pageBuilder.setBoolean(column, value);
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
}
