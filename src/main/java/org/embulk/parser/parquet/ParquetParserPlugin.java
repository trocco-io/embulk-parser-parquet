package org.embulk.parser.parquet;

import org.apache.avro.generic.GenericRecord;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.parquet.ParquetUtil;
import org.embulk.parser.parquet.getter.TimestampUnit;
import org.embulk.spi.*;
import org.embulk.util.config.*;
import org.embulk.util.config.units.ColumnConfig;
import org.embulk.util.config.units.SchemaConfig;
import org.embulk.util.file.FileInputInputStream;
import org.embulk.util.timestamp.TimestampFormatter;

import java.util.Arrays;
import java.util.List;

public class ParquetParserPlugin implements ParserPlugin {
    static final int READ_BUFFER_SIZE = 100*1024*1024;

    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();

    public interface PluginTask extends Task {
        @Config("columns")
        SchemaConfig getColumns();

        @Config("default_timezone")
        @ConfigDefault("\"UTC\"")
        String getDefaultTimeZoneId();

        @Config("default_timestamp_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%N %z\"")
        String getDefaultTimestampFormat();
    }

    @Override
    public void transaction(ConfigSource config, Control control) {
        final ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
        final PluginTask task = configMapper.map(config, PluginTask.class);
        final Schema schema = task.getColumns().toSchema();
        control.run(task.toTaskSource(), schema);
    }

    private PageBuilder newPageBuilder(BufferAllocator bufferAllocator, Schema schema, PageOutput output) {
        // TODO: use Exec.getPageBuilder(bufferAllocator, schema, output) after embulk v0.10
        return new PageBuilder(bufferAllocator, schema, output);
    }

    public void run(TaskSource taskSource, Schema schema, FileInput input, PageOutput output, BufferAllocator bufferAllocator) {
        final TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
        final PluginTask task = taskMapper.map(taskSource, PluginTask.class);

        final ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
        final List<ColumnConfig> columnConfigs = task.getColumns().getColumns();
        final TimestampFormatter[] timestampFormatters = ParquetParserUtil.newTimestampFormatters(
                configMapper, columnConfigs, task.getDefaultTimestampFormat(), task.getDefaultTimeZoneId());
        final TimestampUnit[] timestampUnits = ParquetParserUtil.newTimestampUnits(configMapper, columnConfigs);

        try (FileInputInputStream fileInputInputStream = new FileInputInputStream(input);
             final PageBuilder pageBuilder = newPageBuilder(bufferAllocator, schema, output)) {
            while(fileInputInputStream.nextFile()) {
                final List<GenericRecord> records = ParquetUtil.fetchRecords(readAllBytes(fileInputInputStream));
                for (GenericRecord record: records) {
                    ParquetParserUtil.addRecordToPageBuilder(record, pageBuilder, schema.getColumns(), timestampFormatters, timestampUnits);
                }
            }
            pageBuilder.finish();
        }
    }
    @Override
    public void run(TaskSource taskSource, Schema schema, FileInput input, PageOutput output) {
        // When test, Exec.getBufferAllocator cause error, use runtime.getBufferAllocator
        run(taskSource, schema, input, output, Exec.getBufferAllocator());
    }

    private byte[] readAllBytes(FileInputInputStream fileInputInputStream) {
        byte[] buffer = new byte[READ_BUFFER_SIZE];
        int len = fileInputInputStream.read(buffer, 0, buffer.length);
        if (len == READ_BUFFER_SIZE) {
            throw new RuntimeException(String.format("over buffer size %d", READ_BUFFER_SIZE));
        }
        return Arrays.copyOf(buffer, len);
    }
}
