# Changelog

## 0.0.3

- Support Parquet `BYTE_ARRAY` and `FIXED_LEN_BYTE_ARRAY` columns that have no string annotation.
  They used to fail the whole guess and the whole transfer with `bytes is not supported` /
  `fixed is not supported`, which made a file unreadable even when only one column was binary.
  They are now read as a lowercase hexadecimal string.
- Decode `BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY` annotated with the `DECIMAL` logical type into a
  decimal number rather than the hexadecimal of its two's complement. MySQL `BIGINT UNSIGNED`
  columns exported to Parquet always take this path.
- Read nested `BYTES` / `FIXED` values inside a `RECORD`, `MAP` or `ARRAY` as a hexadecimal string
  too, instead of raising `Unknown type`.
- Raise a `DataException` instead of a `NullPointerException` when a configured column is missing
  from the Parquet schema.
