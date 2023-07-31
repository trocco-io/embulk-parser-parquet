package org.embulk.parser.parquet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.embulk.spi.BufferAllocator;
import org.embulk.test.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.FileInput;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.type.Type;
import org.embulk.spi.util.Pages;
import org.embulk.test.TestPageBuilderReader;
import org.embulk.util.file.InputStreamFileInput;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.embulk.spi.type.Types.*;
import static org.junit.Assert.assertEquals;

public class TestParquetParserPlugin {
    @Rule
    public EmbulkTestRuntime runtime;

    @Before
    public void setUp() {
        runtime = new EmbulkTestRuntime();
    }

    public ConfigSource createTransactionConfigSource(SchemaConfig schema)
    {
        return runtime.getExec().newConfigSource().deepCopy()
                .set("type", "parquet")
                .set("columns", schema);
    }

    private FileInput fileInputs(String... paths)
    {
        FileInputStream[] streams = Arrays.stream(paths).map(path -> {
            File file = new File(Paths.get(path).toUri());
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

    private List<Object[]> fetchRecords(SchemaConfig columnSchema) {
        return fetchRecords(columnSchema, null, null);
    }
    private List<Object[]> fetchRecords(SchemaConfig columnSchema, String defaultTimezone, String defaultTimestampFormat) {
        TestPageBuilderReader.MockPageOutput mockPageOutput = new TestPageBuilderReader.MockPageOutput();
        ParquetParserPlugin plugin = new ParquetParserPlugin();
        ConfigSource configSource = createTransactionConfigSource(columnSchema);
        if(defaultTimezone != null) {
            configSource.set("default_timezone", defaultTimezone);
        }
        if(defaultTimestampFormat != null) {
            configSource.set("default_timestamp_format", defaultTimestampFormat);
        }
        FileInput fileInput = fileInputs("src/test/resources/sample.parquet");
        BufferAllocator bufferAllocator = runtime.getBufferAllocator();
        plugin.transaction(configSource, ((taskSource, schema) -> plugin.run(taskSource, schema, fileInput, mockPageOutput, bufferAllocator)));
        return Pages.toObjects(columnSchema.toSchema(), mockPageOutput.pages);
    }

    @Test
    public void testRunTimestampUnit() {
        final SchemaConfig columnSchema =  schema(
                column("datetime", TIMESTAMP, "timestamp_unit", "second"),
                column("datetime", TIMESTAMP, "timestamp_unit", "Second"),
                column("datetime", TIMESTAMP, "timestamp_unit", "sec"),
                column("datetime", TIMESTAMP, "timestamp_unit", "s"),
                column("datetime", TIMESTAMP, "timestamp_unit", "MilliSecond"),
                column("datetime", TIMESTAMP, "timestamp_unit", "millisecond"),
                column("datetime", TIMESTAMP, "timestamp_unit", "milli_second"),
                column("datetime", TIMESTAMP, "timestamp_unit", "milli"),
                column("datetime", TIMESTAMP, "timestamp_unit", "msec"),
                column("datetime", TIMESTAMP, "timestamp_unit", "ms"),
                column("datetime", TIMESTAMP, "timestamp_unit", "MicroSecond"),
                column("datetime", TIMESTAMP, "timestamp_unit", "microsecond"),
                column("datetime", TIMESTAMP, "timestamp_unit", "micro_second"),
                column("datetime", TIMESTAMP, "timestamp_unit", "micro"),
                column("datetime", TIMESTAMP, "timestamp_unit", "usec"),
                column("datetime", TIMESTAMP, "timestamp_unit", "us"),
                column("datetime", TIMESTAMP, "timestamp_unit", "NanoSecond"),
                column("datetime", TIMESTAMP, "timestamp_unit", "nanosecond"),
                column("datetime", TIMESTAMP, "timestamp_unit", "nano_second"),
                column("datetime", TIMESTAMP, "timestamp_unit", "nano"),
                column("datetime", TIMESTAMP, "timestamp_unit", "nsec"),
                column("datetime", TIMESTAMP, "timestamp_unit", "ns")
        );

        List<Object[]> records = fetchRecords(columnSchema);
        assertEquals(1, records.size());

        Object[] record = records.get(0);
        assertEquals(22, record.length);
        assertEquals("1971-11-26 10:40:00 UTC", record[0].toString());
        assertEquals("1971-11-26 10:40:00 UTC", record[1].toString());
        assertEquals("1971-11-26 10:40:00 UTC", record[2].toString());
        assertEquals("1971-11-26 10:40:00 UTC", record[3].toString());
        assertEquals("1970-01-01 16:40:00 UTC", record[4].toString());
        assertEquals("1970-01-01 16:40:00 UTC", record[5].toString());
        assertEquals("1970-01-01 16:40:00 UTC", record[6].toString());
        assertEquals("1970-01-01 16:40:00 UTC", record[7].toString());
        assertEquals("1970-01-01 16:40:00 UTC", record[8].toString());
        assertEquals("1970-01-01 16:40:00 UTC", record[9].toString());
        assertEquals("1970-01-01 00:01:00 UTC", record[10].toString());
        assertEquals("1970-01-01 00:01:00 UTC", record[11].toString());
        assertEquals("1970-01-01 00:01:00 UTC", record[12].toString());
        assertEquals("1970-01-01 00:01:00 UTC", record[13].toString());
        assertEquals("1970-01-01 00:01:00 UTC", record[14].toString());
        assertEquals("1970-01-01 00:01:00 UTC", record[15].toString());
        assertEquals("1970-01-01 00:00:00.060 UTC", record[16].toString());
        assertEquals("1970-01-01 00:00:00.060 UTC", record[17].toString());
        assertEquals("1970-01-01 00:00:00.060 UTC", record[18].toString());
        assertEquals("1970-01-01 00:00:00.060 UTC", record[19].toString());
        assertEquals("1970-01-01 00:00:00.060 UTC", record[20].toString());
        assertEquals("1970-01-01 00:00:00.060 UTC", record[21].toString());
    }

    @Test
    public void testRunChangeDefaultTimestampFormat() {
        final SchemaConfig columnSchema =  schema(
                column("date_string", TIMESTAMP)
        );

        List<Object[]> records = fetchRecords(columnSchema, null, "%Y-%m-%dT%H:%M:%S");
        assertEquals(1, records.size());

        Object[] record = records.get(0);
        assertEquals(1, record.length);
        assertEquals("1970-01-01 16:07:48 UTC", record[0].toString());
    }

    @Test
    public void testRunChangeDefaultTimeZone() {
        final SchemaConfig columnSchema =  schema(
                column("date_string", TIMESTAMP, "format", "%Y-%m-%dT%H:%M:%S")
        );

        List<Object[]> records = fetchRecords(columnSchema, "Asia/Tokyo", null);
        assertEquals(1, records.size());

        Object[] record = records.get(0);
        assertEquals(1, record.length);
        assertEquals("1970-01-01 07:07:48 UTC", record[0].toString());
    }

    @Test
    public void testRunAllColumns() {
        final SchemaConfig columnSchema =  schema(
                column("id", LONG),
                column("string", STRING),
                column("number", DOUBLE),
                column("date_string", TIMESTAMP, "format", "%Y-%m-%dT%H:%M:%S"),
                column("datetime", TIMESTAMP, "timestamp_unit", "second"),
                column("array", JSON),
                column("object", JSON)
        );

        List<Object[]> records = fetchRecords(columnSchema);
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
