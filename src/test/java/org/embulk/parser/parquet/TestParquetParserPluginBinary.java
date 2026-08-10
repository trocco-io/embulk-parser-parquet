package org.embulk.parser.parquet;

import static org.embulk.spi.type.Types.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.embulk.config.ConfigSource;
import org.embulk.parquet.BinaryParquetFixture;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.DataException;
import org.embulk.spi.FileInput;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.type.Type;
import org.embulk.spi.util.Pages;
import org.embulk.test.EmbulkTestRuntime;
import org.embulk.test.TestPageBuilderReader;
import org.embulk.util.file.InputStreamFileInput;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/** Covers Parquet BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY columns, with and without a DECIMAL. */
public class TestParquetParserPluginBinary {
    @Rule public EmbulkTestRuntime runtime;

    private File parquetFile;

    @Before
    public void setUp() throws IOException {
        runtime = new EmbulkTestRuntime();
        parquetFile = File.createTempFile("embulk-parser-parquet-binary", ".parquet");
        parquetFile.deleteOnExit();
        BinaryParquetFixture.write(parquetFile);
    }

    @Test
    public void testRunBinaryColumnsAsString() {
        final SchemaConfig columnSchema =
                schema(
                        column("id", LONG),
                        column("name", STRING),
                        column("geom", STRING),
                        column("chr", STRING),
                        column("fx", STRING),
                        column("amount", STRING),
                        column("price", STRING),
                        column("nested", JSON));

        final List<Object[]> records = fetchRecords(columnSchema);
        assertEquals(2, records.size());

        final Object[] filled = records.get(0);
        assertEquals(1L, filled[0]);
        assertEquals("Tanaka", filled[1].toString());
        // A MySQL POINT stays a raw byte array in Parquet and is read as a hexadecimal string.
        assertEquals(BinaryParquetFixture.POINT_WKB_HEX, filled[2].toString());
        // A MySQL CHAR is exported without a string annotation, so it becomes hexadecimal too.
        assertEquals("6162", filled[3].toString());
        assertEquals("010203ff", filled[4].toString());
        // DECIMAL annotated bytes must be a number, not the hexadecimal of its two's complement.
        assertEquals("18446744073709551615", filled[5].toString());
        assertEquals("-1.23", filled[6].toString());
        assertEquals("{\"inner\":\"6162\"}", filled[7].toString());

        final Object[] empty = records.get(1);
        assertEquals(2L, empty[0]);
        for (int i = 1; i < empty.length; i++) {
            assertNull(empty[i]);
        }
    }

    @Test
    public void testRunBinaryColumnAsDouble() {
        final List<Object[]> records = fetchRecords(schema(column("price", DOUBLE)));
        assertEquals(-1.23d, (Double) records.get(0)[0], 0.0001d);
        assertNull(records.get(1)[0]);
    }

    @Test
    public void testRunDecimalColumnAsLongRaisesDataExceptionWhenInexact() {
        // DECIMAL(9,2) -1.23 has no exact long representation.
        assertThrows(DataException.class, () -> fetchRecords(schema(column("price", LONG))));
        // BIGINT UNSIGNED max overflows a signed long.
        assertThrows(DataException.class, () -> fetchRecords(schema(column("amount", LONG))));
    }

    @Test
    public void testRunRawBytesAsLongRaisesDataException() {
        // A hexadecimal string parsed as a number would be silently wrong, so it must not happen.
        assertThrows(DataException.class, () -> fetchRecords(schema(column("geom", LONG))));
    }

    @Test
    public void testRunRawBytesAsTimestampRaisesDataException() {
        assertThrows(DataException.class, () -> fetchRecords(schema(column("geom", TIMESTAMP))));
    }

    @Test
    public void testRunUnknownColumnRaisesDataException() {
        assertThrows(DataException.class, () -> fetchRecords(schema(column("missing", STRING))));
    }

    private ConfigSource createTransactionConfigSource(SchemaConfig schema) {
        return runtime.getExec()
                .newConfigSource()
                .deepCopy()
                .set("type", "parquet")
                .set("columns", schema);
    }

    private FileInput fileInputs(String... paths) {
        final FileInputStream[] streams =
                Arrays.stream(paths)
                        .map(
                                path -> {
                                    try {
                                        return new FileInputStream(new File(path));
                                    } catch (FileNotFoundException e) {
                                        throw new RuntimeException(e);
                                    }
                                })
                        .toArray(FileInputStream[]::new);
        return new InputStreamFileInput(
                runtime.getBufferAllocator(),
                new InputStreamFileInput.IteratorProvider(ImmutableList.copyOf(streams)));
    }

    private SchemaConfig schema(ColumnConfig... columns) {
        return new SchemaConfig(Lists.newArrayList(columns));
    }

    private ColumnConfig column(String name, Type type) {
        return new ColumnConfig(name, type, runtime.getExec().newConfigSource());
    }

    private List<Object[]> fetchRecords(SchemaConfig columnSchema) {
        final TestPageBuilderReader.MockPageOutput mockPageOutput =
                new TestPageBuilderReader.MockPageOutput();
        final ParquetParserPlugin plugin = new ParquetParserPlugin();
        final ConfigSource configSource = createTransactionConfigSource(columnSchema);
        final FileInput fileInput = fileInputs(parquetFile.getAbsolutePath());
        plugin.transaction(
                configSource,
                ((taskSource, schema) ->
                        plugin.run(
                                taskSource,
                                schema,
                                fileInput,
                                mockPageOutput,
                                runtime.getBufferAllocator())));
        return Pages.toObjects(columnSchema.toSchema(), mockPageOutput.pages);
    }
}
