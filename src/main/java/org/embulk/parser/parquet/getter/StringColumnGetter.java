package org.embulk.parser.parquet.getter;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;
import org.embulk.util.json.JsonParser;
import org.embulk.util.timestamp.TimestampFormatter;

import java.time.Instant;

public class StringColumnGetter extends BaseColumnGetter {
    protected String value;
    private final JsonParser jsonParser = new JsonParser();

    private final TimestampFormatter timestampFormatter;

    public StringColumnGetter(PageBuilder pageBuilder, TimestampFormatter timestampFormatter) {
        super(pageBuilder);
        this.timestampFormatter = timestampFormatter;
    }

    @Override
    public void setValue(Object value)
    {
        if (value == null)
            this.value = null;
        else
            this.value = value.toString();
    }

    @Override
    public void booleanColumn(Column column) {
        if (this.value == null) {
            pageBuilder.setNull(column);
        }
        else {
            pageBuilder.setBoolean(column, Boolean.parseBoolean(value));
        }
    }

    @Override
    public void longColumn(Column column) {
        if (this.value == null) {
            pageBuilder.setNull(column);
        }
        else {
            pageBuilder.setLong(column, Long.parseLong(value));
        }
    }

    @Override
    public void doubleColumn(Column column) {
        if (this.value == null) {
            pageBuilder.setNull(column);
        }
        else {
            pageBuilder.setDouble(column, Double.parseDouble(value));
        }
    }

    @Override
    public void stringColumn(Column column) {
        if (this.value == null) {
            pageBuilder.setNull(column);
        }
        else {
            pageBuilder.setString(column, value);
        }
    }

    @Override
    public void timestampColumn(Column column) {
        if (this.value == null) {
            pageBuilder.setNull(column);
        }
        else {
            Instant instant = timestampFormatter.parse(value);
            Timestamp timestamp = Timestamp.ofInstant(instant);
            // TODO use Instant directly after embulk v.0.10
            pageBuilder.setTimestamp(column, timestamp);
        }
    }

    @Override
    public void jsonColumn(Column column) {
        if (this.value == null) {
            pageBuilder.setNull(column);
        }
        else {
            pageBuilder.setJson(column, jsonParser.parse(value));
        }
    }
}
