package org.embulk.parser.parquet.getter;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.DataException;
import org.embulk.spi.PageBuilder;

public class BaseColumnGetter implements ColumnVisitor {
    protected final PageBuilder pageBuilder;
    protected Object value;

    public BaseColumnGetter(PageBuilder pageBuilder) {
        this.pageBuilder = pageBuilder;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public void booleanColumn(Column column) {
        if (value == null) {
            pageBuilder.setNull(column);
        } else {
            throw new DataException(
                    String.format("cannot convert value from %s", column.getType()));
        }
    }

    @Override
    public void longColumn(Column column) {
        if (value == null) {
            pageBuilder.setNull(column);
        } else {
            throw new DataException(
                    String.format("cannot convert value from %s", column.getType()));
        }
    }

    @Override
    public void doubleColumn(Column column) {
        if (value == null) {
            pageBuilder.setNull(column);
        } else {
            throw new DataException(
                    String.format("cannot convert value from %s", column.getType()));
        }
    }

    @Override
    public void stringColumn(Column column) {
        if (value == null) {
            pageBuilder.setNull(column);
        } else {
            throw new DataException(
                    String.format("cannot convert value from %s", column.getType()));
        }
    }

    @Override
    public void timestampColumn(Column column) {
        if (value == null) {
            pageBuilder.setNull(column);
        } else {
            throw new DataException(
                    String.format("cannot convert value from %s", column.getType()));
        }
    }

    @Override
    public void jsonColumn(Column column) {
        if (value == null) {
            pageBuilder.setNull(column);
        } else {
            throw new DataException(
                    String.format("cannot convert value from %s", column.getType()));
        }
    }
}
