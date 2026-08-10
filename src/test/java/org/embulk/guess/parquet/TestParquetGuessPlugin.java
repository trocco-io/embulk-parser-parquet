package org.embulk.guess.parquet;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.parquet.BinaryParquetFixture;
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

    @Test
    public void testGuessBinaryColumns() throws IOException {
        // Before BYTES / FIXED were mapped, a single binary column made the whole guess fail with
        // "bytes is not supported", so the user could not even build a transfer setting.
        File file = File.createTempFile("embulk-parser-parquet-guess", ".parquet");
        file.deleteOnExit();
        BinaryParquetFixture.write(file);

        ConfigDiff configDiff = guess(Files.readAllBytes(file.toPath()));
        ConfigDiff parserDiff = configDiff.getNested("parser");
        assertEquals(parserDiff.get(String.class, "type"), "parquet");
        List<Map> columns = parserDiff.getListOf(Map.class, "columns");
        assertEquals(columns.size(), 8);
        assertNameAndType(columns.get(0), "id", "long");
        assertNameAndType(columns.get(1), "name", "string");
        assertNameAndType(columns.get(2), "geom", "string");
        assertNameAndType(columns.get(3), "chr", "string");
        assertNameAndType(columns.get(4), "fx", "string");
        assertNameAndType(columns.get(5), "amount", "string");
        assertNameAndType(columns.get(6), "price", "string");
        assertNameAndType(columns.get(7), "nested", "json");
    }

    @Test
    public void testGuessInvalidFile() {
        ConfigDiff configDiff = guess("/invalid.parquet");
        assertTrue(configDiff.getNestedOrGetEmpty("parser").isEmpty());
    }

    private void assertNameAndType(Map column, String name, String type) {
        assertEquals(column.get("name"), name);
        assertEquals(column.get("type"), type);
    }

    private ConfigDiff guess(String resource) {
        try {
            InputStream is = this.getClass().getResourceAsStream(resource);
            return guess(IOUtils.toByteArray(is));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private ConfigDiff guess(byte[] bytes) {
        ConfigSource configSource = runtime.getExec().newConfigSource();
        ParquetGuessPlugin plugin = new ParquetGuessPlugin();
        return plugin.guess(configSource, Buffer.wrap(bytes));
    }
}
