package org.embulk.parser.parquet;

import org.apache.avro.generic.GenericRecord;
import org.embulk.parser.parquet.getter.BaseColumnGetter;
import org.embulk.parser.parquet.getter.ColumnGetterFactory;
import org.embulk.parser.parquet.getter.TimestampUnit;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.Task;
import org.embulk.util.config.units.ColumnConfig;
import org.embulk.util.timestamp.TimestampFormatter;

import java.util.List;
import java.util.stream.Stream;

public class ParquetParserUtil {

    static public void addRecordToPageBuilder(GenericRecord record, PageBuilder pageBuilder, List<Column> columns, TimestampFormatter[] timestampFormatters, TimestampUnit[] timestampUnits) {
        ColumnGetterFactory factory = new ColumnGetterFactory(record.getSchema(), pageBuilder, timestampFormatters, timestampUnits);
        for (Column column: columns) {
            BaseColumnGetter columnGetter = factory.newColumnGetter(column);
            columnGetter.setValue(record.get(column.getName()));
            column.visit(columnGetter);
        }
        pageBuilder.addRecord();
    }
    private interface TimestampColumnOption extends Task {
        // FIXME: java.util.Optional<String> cause RunTimeError, maybe solve with shadow jar
        @Config("timezone")
        @ConfigDefault("\"\"")
        String getTimeZoneId();

        @Config("format")
        @ConfigDefault("\"\"")
        String getFormat();

        @Config(value = "timestamp_unit")
        @ConfigDefault("\"second\"")
        TimestampUnit getTimestampUnit();
    }

    static public TimestampFormatter[] newTimestampFormatters(ConfigMapper configMapper,
                                                              List<ColumnConfig> columns,
                                                              String defaultTimestampFormat,
                                                              String defaultTimeZoneId) {
        return timestampColumnOptions(configMapper, columns).map(columnOption -> {
            if(columnOption == null) {
                return null;
            }
            final String format = columnOption.getFormat().length() != 0 ? columnOption.getFormat() : defaultTimestampFormat;
            final String tz = columnOption.getTimeZoneId().length() != 0 ? columnOption.getTimeZoneId() : defaultTimeZoneId;
            return TimestampFormatter.builder(format, true).setDefaultZoneFromString(tz).build();
        }).toArray(TimestampFormatter[]::new);
    }

    static public TimestampUnit[] newTimestampUnits(ConfigMapper configMapper, List<ColumnConfig> columns) {
        return timestampColumnOptions(configMapper, columns).map(columnOption ->
            columnOption != null ? columnOption.getTimestampUnit() : null
        ).toArray(TimestampUnit[]::new);
    }

    static private Stream<TimestampColumnOption> timestampColumnOptions(ConfigMapper configMapper, List<ColumnConfig> columns) {
        return columns.stream().map(column -> {
            if (!(column.getType() instanceof org.embulk.spi.type.TimestampType)) {
                return null;
            }
            return configMapper.map(column.getOption(), TimestampColumnOption.class);
        });
    }
}
