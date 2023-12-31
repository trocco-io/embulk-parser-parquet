package org.embulk.parser.parquet;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;
import org.embulk.parquet.ParquetUtil;
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

public class ParquetParserUtil {
    public static void addRecordsToPageBuilder(
            InputStream inputStream,
            PageBuilder pageBuilder,
            List<Column> columns,
            TimestampFormatter[] timestampFormatters,
            TimestampUnit[] timestampUnits) {
        final ParquetReader<Object> reader = ParquetUtil.buildReader(inputStream);
        try {
            while (true) {
                final Object obj = reader.read();
                if (obj == null) {
                    return;
                }
                if (obj instanceof GenericRecord) {
                    GenericRecord record = (GenericRecord) obj;
                    addRecordToPageBuilder(
                            record, pageBuilder, columns, timestampFormatters, timestampUnits);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void addRecordToPageBuilder(
            GenericRecord record,
            PageBuilder pageBuilder,
            List<Column> columns,
            TimestampFormatter[] timestampFormatters,
            TimestampUnit[] timestampUnits) {
        ColumnGetterFactory factory =
                new ColumnGetterFactory(
                        record.getSchema(), pageBuilder, timestampFormatters, timestampUnits);
        for (Column column : columns) {
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

    public static TimestampFormatter[] newTimestampFormatters(
            ConfigMapper configMapper,
            List<ColumnConfig> columns,
            String defaultTimestampFormat,
            String defaultTimeZoneId) {
        return timestampColumnOptions(configMapper, columns)
                .map(
                        columnOption -> {
                            if (columnOption == null) {
                                return null;
                            }
                            final String format =
                                    columnOption.getFormat().length() != 0
                                            ? columnOption.getFormat()
                                            : defaultTimestampFormat;
                            final String tz =
                                    columnOption.getTimeZoneId().length() != 0
                                            ? columnOption.getTimeZoneId()
                                            : defaultTimeZoneId;
                            return TimestampFormatter.builder(format, true)
                                    .setDefaultZoneFromString(tz)
                                    .build();
                        })
                .toArray(TimestampFormatter[]::new);
    }

    public static TimestampUnit[] newTimestampUnits(
            ConfigMapper configMapper, List<ColumnConfig> columns) {
        return timestampColumnOptions(configMapper, columns)
                .map(columnOption -> columnOption != null ? columnOption.getTimestampUnit() : null)
                .toArray(TimestampUnit[]::new);
    }

    private static Stream<TimestampColumnOption> timestampColumnOptions(
            ConfigMapper configMapper, List<ColumnConfig> columns) {
        return columns.stream()
                .map(
                        column -> {
                            if (!(column.getType() instanceof org.embulk.spi.type.TimestampType)) {
                                return null;
                            }
                            return configMapper.map(
                                    column.getOption(), TimestampColumnOption.class);
                        });
    }
}
