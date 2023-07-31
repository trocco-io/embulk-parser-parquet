package org.embulk.guess.parquet;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Buffer;
import org.embulk.test.EmbulkTestRuntime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TestParquetGuessPlugin {
    @Rule public EmbulkTestRuntime runtime;

    @Before
    public void setUp() {
        runtime = new EmbulkTestRuntime();
    }

    @Test
    public void testGuess() {
        ConfigDiff configDiff = guess("/sample.parquet");
        ConfigDiff parserDiff = configDiff.getNested("parser");
        assertEquals(parserDiff.get(String.class, "type"), "parquet");
        List<Map> columns = parserDiff.getListOf(Map.class, "columns");
        assertEquals(columns.size(), 7);
        assertNameAndType(columns.get(0), "id", "long");
        assertNameAndType(columns.get(1), "string", "string");
        assertNameAndType(columns.get(2), "number", "double");
        assertNameAndType(columns.get(3), "date_string", "string");
        assertNameAndType(columns.get(4), "datetime", "long");
        assertNameAndType(columns.get(5), "array", "json");
        assertNameAndType(columns.get(6), "object", "json");
    }

    private void assertNameAndType(Map column, String name, String type) {
        assertEquals(column.get("name"), name);
        assertEquals(column.get("type"), type);
    }

    private ConfigDiff guess(String resource) {
        ConfigSource configSource = runtime.getExec().newConfigSource();
        try {
            InputStream is = this.getClass().getResourceAsStream(resource);
            Buffer sample = Buffer.wrap(IOUtils.toByteArray(is));
            ParquetGuessPlugin plugin = new ParquetGuessPlugin();
            return plugin.guess(configSource, sample);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
