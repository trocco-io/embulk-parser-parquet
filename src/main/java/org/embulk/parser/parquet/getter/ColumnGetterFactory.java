package org.embulk.parser.parquet.getter;

import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;

import org.apache.avro.Schema;
import org.embulk.parquet.ParquetUtil;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.PageBuilder;
import org.embulk.util.timestamp.TimestampFormatter;

public class ColumnGetterFactory {
    private final Schema parquetSchema;
    private final PageBuilder pageBuilder;
    private final TimestampFormatter[] timestampFormatters;
    private final TimestampUnit[] timestampUnits;

    public ColumnGetterFactory(
            Schema parquetSchema,
            PageBuilder pageBuilder,
            TimestampFormatter[] timestampFormatters,
            TimestampUnit[] timestampUnits) {
        this.parquetSchema = parquetSchema;
        this.pageBuilder = pageBuilder;
        this.timestampFormatters = timestampFormatters;
        this.timestampUnits = timestampUnits;
    }

    public BaseColumnGetter newColumnGetter(Column column) {
        int index = column.getIndex();
        final TimestampFormatter timestampFormatter = timestampFormatters[index];
        final TimestampUnit timestampUnit = timestampUnits[index];
        final Schema fieldSchema = parquetSchema.getField(column.getName()).schema();
        final Schema.Type type = ParquetUtil.fetchType(fieldSchema);
        return getColumnGetterFromTypeName(type, timestampFormatter, timestampUnit);
    }

    private BaseColumnGetter getColumnGetterFromTypeName(
            Schema.Type type, TimestampFormatter timestampFormatter, TimestampUnit timestampUnit) {
        switch (type) {
            case STRING:
            case ENUM:
            case NULL:
                return new StringColumnGetter(pageBuilder, timestampFormatter);
            case INT:
                return new IntegerColumnGetter(pageBuilder, timestampUnit);
            case LONG:
                return new LongColumnGetter(pageBuilder, timestampUnit);
            case FLOAT:
                return new FloatColumnGetter(pageBuilder, timestampUnit);
            case DOUBLE:
                return new DoubleColumnGetter(pageBuilder, timestampUnit);
            case BOOLEAN:
                return new BooleanColumnGetter(pageBuilder);
            case ARRAY:
            case MAP:
            case RECORD:
                return new GenericDataColumnGetter(pageBuilder);
            default:
                throw new DataException(String.format("%s is not supported", type.getName()));
        }
    }
}
