package org.embulk.parser.parquet;

import java.util.List;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.parser.parquet.getter.TimestampUnit;
import org.embulk.spi.*;
import org.embulk.util.config.*;
import org.embulk.util.config.units.ColumnConfig;
import org.embulk.util.config.units.SchemaConfig;
import org.embulk.util.file.FileInputInputStream;
import org.embulk.util.timestamp.TimestampFormatter;

public class ParquetParserPlugin implements ParserPlugin {

    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
            ConfigMapperFactory.builder().addDefaultModules().build();

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

    private PageBuilder newPageBuilder(
            BufferAllocator bufferAllocator, Schema schema, PageOutput output) {
        // TODO: use Exec.getPageBuilder(bufferAllocator, schema, output) after embulk v0.10
        return new PageBuilder(bufferAllocator, schema, output);
    }

    public void run(
            TaskSource taskSource,
            Schema schema,
            FileInput input,
            PageOutput output,
            BufferAllocator bufferAllocator) {
        final TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
        final PluginTask task = taskMapper.map(taskSource, PluginTask.class);

        final ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
        final List<ColumnConfig> columnConfigs = task.getColumns().getColumns();
        final TimestampFormatter[] timestampFormatters =
                ParquetParserUtil.newTimestampFormatters(
                        configMapper,
                        columnConfigs,
                        task.getDefaultTimestampFormat(),
                        task.getDefaultTimeZoneId());
        final TimestampUnit[] timestampUnits =
                ParquetParserUtil.newTimestampUnits(configMapper, columnConfigs);

        try (FileInputInputStream fileInputInputStream = new FileInputInputStream(input);
                final PageBuilder pageBuilder = newPageBuilder(bufferAllocator, schema, output)) {
            while (fileInputInputStream.nextFile()) {
                ParquetParserUtil.addRecordsToPageBuilder(
                        fileInputInputStream,
                        pageBuilder,
                        schema.getColumns(),
                        timestampFormatters,
                        timestampUnits);
            }
            pageBuilder.finish();
        }
    }

    @Override
    public void run(TaskSource taskSource, Schema schema, FileInput input, PageOutput output) {
        // When test, Exec.getBufferAllocator cause error, use runtime.getBufferAllocator
        run(taskSource, schema, input, output, Exec.getBufferAllocator());
    }
}
