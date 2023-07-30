package org.embulk.parser.parquet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.embulk.test.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.FileInput;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.type.Type;
import org.embulk.spi.util.Pages;
import org.embulk.test.TestPageBuilderReader;
import org.embulk.util.file.InputStreamFileInput;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;

import static org.embulk.spi.type.Types.*;
import static org.junit.Assert.assertEquals;

public class TestParquetParserPlugin {
    @Rule
    public EmbulkTestRuntime runtime;

    public ConfigSource createTransactionConfigSource(SchemaConfig schema)
    {
        return runtime.getExec().newConfigSource().deepCopy()
                .set("type", "parquet")
                .set("columns", schema);
    }

    private FileInput fileInputs(String... paths)
    {
        FileInputStream[] streams = Arrays.stream(paths).map(path -> {
            File file = new File(this.getClass().getResource(path).getPath());
            try {
                return new FileInputStream(file);
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }).toArray(FileInputStream[]::new);
        InputStreamFileInput.Provider provider = new InputStreamFileInput.IteratorProvider(
                ImmutableList.copyOf(streams));
        return new InputStreamFileInput(runtime.getBufferAllocator(), provider);
    }

    private SchemaConfig schema(ColumnConfig... columns)
    {
        return new SchemaConfig(Lists.newArrayList(columns));
    }

    private ColumnConfig column(String name, Type type)
    {
        return column(name, type, runtime.getExec().newConfigSource());
    }

    private ColumnConfig column(String name, Type type, String optionKey, String optionValue)
    {
        ConfigSource option = runtime.getExec().newConfigSource().set(optionKey, optionValue);
        return column(name, type, option);
    }

    private ColumnConfig column(String name, Type type, ConfigSource option)
    {
        return new ColumnConfig(name, type, option);
    }

    @Test
    public void testRun() {
        runtime = new EmbulkTestRuntime();
        final SchemaConfig columnSchema =  schema(
                column("id", LONG),
                column("string", STRING),
                column("number", DOUBLE),
                column("date_string", TIMESTAMP, "format", "%Y-%m-%dT%H:%M:%S"),
                column("datetime", TIMESTAMP, "timestamp_unit", "second"),
                column("array", JSON),
                column("object", JSON)
        );
        TestPageBuilderReader.MockPageOutput mockPageOutput = new TestPageBuilderReader.MockPageOutput();
        ParquetParserPlugin plugin = new ParquetParserPlugin();
        plugin.transaction(
                createTransactionConfigSource(columnSchema),
                ((taskSource, schema) ->
                        plugin.run(taskSource, schema, fileInputs("sample.parquet"), mockPageOutput, runtime.getBufferAllocator())));
        List<Object[]> records = Pages.toObjects(columnSchema.toSchema(), mockPageOutput.pages);
        assertEquals(1, records.size());

        Object[] record = records.get(0);
        assertEquals(7, record.length);
        assertEquals(1L, record[0]);
        assertEquals("Tanaka", record[1]);
        assertEquals(3.5, record[2]);
        assertEquals("1970-01-01 16:07:48 UTC", record[3].toString());
        assertEquals("1971-11-26 10:40:00 UTC", record[4].toString());
        assertEquals("[{\"item\":\"tag0\"},{\"item\":\"tag1\"}]", record[5].toString());
        assertEquals("{\"key0\":\"k\"}", record[6].toString());
    }
}
