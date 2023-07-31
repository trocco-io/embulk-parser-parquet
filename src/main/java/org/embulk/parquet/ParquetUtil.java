package org.embulk.parquet;

import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ParquetUtil {
    private static final byte[] PARQUET_HEADER = {0x50, 0x41, 0x52, 0x31};
    static public boolean isParquetFile(byte[] bytes) {
        if(PARQUET_HEADER.length > bytes.length) {
            return false;
        }
        for(int i = 0; i < PARQUET_HEADER.length; i++) {
            if(PARQUET_HEADER[i] != bytes[i]) {
                return false;
            }
        }
        return true;
    }

    static public GenericRecord fetchRecord(byte[] bytes) {
        final ParquetReader<Object> reader = buildReader(bytes);
        try {
            while(true) {
                final Object obj = reader.read();
                if(obj == null) {
                    return null;
                }
                if (obj instanceof GenericRecord) {
                    return ((GenericRecord) obj);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static public List<GenericRecord> fetchRecords(byte[] bytes) {
        final ParquetReader<Object> reader = buildReader(bytes);
        List<GenericRecord> records = new ArrayList<>();
        try {
            while(true) {
                final Object obj = reader.read();
                if(obj == null) {
                    break;
                }
                if (obj instanceof GenericRecord) {
                    records.add(((GenericRecord) obj));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return records;
    }

    static private ParquetReader<Object> buildReader(byte[] bytes) {
        InputFile parquetStream = new ParquetStream(bytes);
        try {
            Builder<Object> builder = new Builder<>(parquetStream);
            return builder.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName()).build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // AVROParquetReader.java
    public static class Builder<T> extends ParquetReader.Builder<T> {
        private Builder(InputFile file) {
            super(file);
        }

        @Override
        protected ReadSupport<T> getReadSupport() {
            conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false);
            return new AvroReadSupport<T>(null);
        }
    }

    // https://stackoverflow.com/questions/58141248/read-parquet-data-from-bytearrayoutputstream-instead-of-file
    private static class ParquetStream implements InputFile {
        private final byte[] data;

        private class SeekableByteArrayInputStream extends ByteArrayInputStream {
            public SeekableByteArrayInputStream(byte[] buf) {
                super(buf);
            }

            public void setPos(int pos) {
                this.pos = pos;
            }

            public int getPos() {
                return this.pos;
            }
        }

        public ParquetStream(byte[] bytes) {
            this.data = bytes;
        }

        @Override
        public long getLength() {
            return this.data.length;
        }

        @Override
        public SeekableInputStream newStream() {
            return new DelegatingSeekableInputStream(new SeekableByteArrayInputStream(this.data)) {
                @Override
                public void seek(long newPos) {
                    ((SeekableByteArrayInputStream) this.getStream()).setPos((int) newPos);
                }

                @Override
                public long getPos() {
                    return ((SeekableByteArrayInputStream) this.getStream()).getPos();
                }
            };
        }
    }
}
