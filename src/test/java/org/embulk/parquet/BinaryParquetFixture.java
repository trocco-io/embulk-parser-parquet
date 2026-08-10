package org.embulk.parquet;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

/**
 * Writes a Parquet file that contains the binary flavored types an Aurora MySQL snapshot export
 * produces: BYTE_ARRAY without an annotation (geometry, CHAR, BLOB) and FIXED_LEN_BYTE_ARRAY with
 * and without a DECIMAL annotation (BIGINT UNSIGNED).
 *
 * <p>The file is written from a raw {@link MessageType} rather than through AvroParquetWriter on
 * purpose. AvroParquetWriter stores a {@code parquet.avro.schema} metadata entry, which makes the
 * reader skip AvroSchemaConverter and therefore not reproduce what production files do.
 */
public final class BinaryParquetFixture {
    /** A MySQL POINT value: 4 byte SRID (4326, little endian) followed by the standard WKB. */
    public static final byte[] POINT_WKB =
            hexToBytes("e6100000" + "0101000000" + "5f984c158c786140" + "8104c58f31d74140");

    public static final String POINT_WKB_HEX = "e610000001010000005f984c158c7861408104c58f31d74140";

    /** "ab" stored in a MySQL CHAR column, which is exported without a string annotation. */
    public static final byte[] CHAR_BYTES = {0x61, 0x62};

    public static final byte[] FIXED_BYTES = {0x01, 0x02, 0x03, (byte) 0xff};

    /** 18446744073709551615, the MySQL BIGINT UNSIGNED maximum, as DECIMAL(20,0). */
    public static final BigInteger BIG_UNSIGNED = new BigInteger("18446744073709551615");

    private BinaryParquetFixture() {}

    public static MessageType messageType() {
        return Types.buildMessage()
                .required(PrimitiveTypeName.INT64)
                .named("id")
                .optional(PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("name")
                .optional(PrimitiveTypeName.BINARY)
                .named("geom")
                .optional(PrimitiveTypeName.BINARY)
                .named("chr")
                .optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                .length(4)
                .named("fx")
                .optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                .length(9)
                .as(LogicalTypeAnnotation.decimalType(0, 20))
                .named("amount")
                .optional(PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.decimalType(2, 9))
                .named("price")
                .optionalGroup()
                .optional(PrimitiveTypeName.BINARY)
                .named("inner")
                .named("nested")
                .named("root");
    }

    /** Writes two records: one with every column filled in, one with only the required id. */
    public static File write(File file) throws IOException {
        if (file.exists() && !file.delete()) {
            throw new IOException("cannot overwrite " + file);
        }
        final MessageType messageType = messageType();
        final Configuration configuration = new Configuration();
        configuration.set("fs.file.impl", LocalFileSystem.class.getName());

        try (ParquetWriter<Group> writer =
                ExampleParquetWriter.builder(new Path(file.getAbsolutePath()))
                        .withConf(configuration)
                        .withType(messageType)
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        .build()) {
            final SimpleGroupFactory factory = new SimpleGroupFactory(messageType);

            final Group filled =
                    factory.newGroup()
                            .append("id", 1L)
                            .append("name", "Tanaka")
                            .append("geom", Binary.fromConstantByteArray(POINT_WKB))
                            .append("chr", Binary.fromConstantByteArray(CHAR_BYTES))
                            .append("fx", Binary.fromConstantByteArray(FIXED_BYTES))
                            .append(
                                    "amount",
                                    Binary.fromConstantByteArray(twosComplement(BIG_UNSIGNED, 9)))
                            // Negative so the sign-restoring path of new BigInteger(bytes) is
                            // exercised: -123 is stored as the two's complement byte 0x85.
                            .append(
                                    "price",
                                    Binary.fromConstantByteArray(
                                            BigInteger.valueOf(-123).toByteArray()));
            filled.addGroup("nested").append("inner", Binary.fromConstantByteArray(CHAR_BYTES));
            writer.write(filled);

            writer.write(factory.newGroup().append("id", 2L));
        }
        return file;
    }

    /** Left pads the two's complement representation of value to exactly length bytes. */
    static byte[] twosComplement(BigInteger value, int length) {
        final byte[] source = value.toByteArray();
        if (source.length > length) {
            throw new IllegalArgumentException(value + " does not fit in " + length + " bytes");
        }
        final byte[] padded = new byte[length];
        final byte sign = (byte) (value.signum() < 0 ? 0xff : 0x00);
        for (int i = 0; i < length - source.length; i++) {
            padded[i] = sign;
        }
        System.arraycopy(source, 0, padded, length - source.length, source.length);
        return padded;
    }

    static byte[] hexToBytes(String hex) {
        final byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return bytes;
    }
}
