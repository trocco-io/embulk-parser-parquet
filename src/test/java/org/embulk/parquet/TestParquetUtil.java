package org.embulk.parquet;

import static org.embulk.parquet.ParquetUtil.isParquetFile;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestParquetUtil {
    @Test
    public void testIsParquet() {
        assertTrue(isParquetFile(new byte[] {0x50, 0x41, 0x52, 0x31, 0x50, 0x41, 0x52, 0x31}));
        assertTrue(isParquetFile(new byte[] {0x50, 0x41, 0x52, 0x31, 0, 0x50, 0x41, 0x52, 0x31}));
        assertFalse(isParquetFile(new byte[] {0x50, 0x41, 0x52, 0, 0, 0x50, 0x41, 0x52, 0x31}));
        assertFalse(isParquetFile(new byte[] {0x50, 0x41, 0x52, 0x31}));
    }
}
