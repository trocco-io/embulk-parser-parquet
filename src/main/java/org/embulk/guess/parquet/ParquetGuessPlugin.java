package org.embulk.guess.parquet;

import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.parquet.ParquetUtil;
import org.embulk.spi.Buffer;
import org.embulk.spi.GuessPlugin;
import org.embulk.util.config.ConfigMapperFactory;

public class ParquetGuessPlugin implements GuessPlugin {

    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
            ConfigMapperFactory.builder().addDefaultModules().build();

    @Override
    public ConfigDiff guess(ConfigSource config, Buffer sample) {
        final ConfigDiff configDiff = CONFIG_MAPPER_FACTORY.newConfigDiff();

        final byte[] bytes = copyBuffer(sample, sample.limit());
        if (!ParquetUtil.isParquetFile(bytes)) {
            return configDiff;
        }
        final GenericRecord record = ParquetUtil.fetchRecord(bytes);
        if (record == null) {
            return configDiff;
        }
        ConfigDiff parserConfig =
                configDiff.set("parser", Collections.emptyMap()).getNested("parser");
        parserConfig.set("type", "parquet");
        final List<Schema.Field> fields = record.getSchema().getFields();
        parserConfig.set("columns", ParquetGuessUtil.createColumns(fields));

        return configDiff;
    }

    private byte[] copyBuffer(Buffer buffer, int size) {
        byte[] bytes = new byte[size];
        buffer.getBytes(0, bytes, 0, size);
        return bytes;
    }
}
