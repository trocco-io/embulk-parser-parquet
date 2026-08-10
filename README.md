# Parquet parser plugin for Embulk

Parquet parser plugin for Embulk.

## Overview

* **Plugin type**: parser
* **Guess supported**: yes

## Configuration

- **type**: Specify this parser as parquet
- **columns**: Specify column name and type. See below (array, required)
  - timestamp_unit: Specify unit of time. (This config is effective only if parquet value is `long`, `int`, `float`, `double`)
* **default_timezone**: Default timezone of the timestamp (string, default: UTC)
* **default_timestamp_format**: Default timestamp format of the timestamp (string, default: `%Y-%m-%d %H:%M:%S.%N %z`)

### Binary columns

A Parquet `BYTE_ARRAY` or `FIXED_LEN_BYTE_ARRAY` column that has **no string annotation** is read as
a **lowercase hexadecimal string**, and guess reports it as `string`. Only `string` is supported for
those columns; asking for `long`, `double`, `boolean`, `timestamp` or `json` raises a
`DataException` instead of silently producing a wrong value.

When the column is annotated with the **`DECIMAL` logical type**, the two's complement byte array is
decoded into a decimal number instead, so guess still reports `string` but the value is the plain
decimal representation (for example `18446744073709551615`, not its hexadecimal). Such a column can
also be read as `double`, or as `long` when the value has an exact long representation.

Known limitation: the `DECIMAL` decoding only applies to top-level columns. A decimal **nested
inside a `RECORD`, `MAP` or `ARRAY`** (read as a `json` column) is emitted as the hexadecimal of its
two's complement, because the nested value conversion dispatches on the runtime class and the
logical type annotation is not available there.

This matters for files produced by the Amazon RDS/Aurora snapshot export to S3, which maps

| MySQL source type | Parquet type | read as |
|---|---|---|
| `POINT`, `GEOMETRY`, `POLYGON`, `LINESTRING`, `MULTI*` | `BYTE_ARRAY` (no annotation) | hexadecimal string of the raw bytes, i.e. the 4 byte SRID followed by the WKB |
| `BINARY`, `VARBINARY`, `BLOB`, `TINY`/`MEDIUM`/`LONGBLOB`, `BIT` | `BYTE_ARRAY` (no annotation) | hexadecimal string |
| `CHAR` | `BYTE_ARRAY` (no annotation) | hexadecimal string — note that `VARCHAR` and `TEXT` do carry a string annotation and stay text |
| `BIGINT UNSIGNED`, large `DECIMAL`/`NUMERIC` | `FIXED_LEN_BYTE_ARRAY` + `DECIMAL(p,s)` | decimal number |

Hexadecimal was chosen over base64 so the value can be fed straight into the geospatial functions of
the destination warehouse, for example Amazon Redshift `ST_GeomFromWKB(col)`, which takes a `VARCHAR`
holding a hexadecimal WKB. Note that MySQL prefixes its geometry values with a 4 byte SRID, so
`SUBSTRING(col, 9)` may be needed before handing the value over.

support `timestamp_unit` type is below.

- "Second"
- "second"
- "sec"
- "s"
- "MilliSecond"
- "millisecond"
- "milli_second"
- "milli"
- "msec"
- "ms"
- "MicroSecond"
- "microsecond"
- "micro_second"
- "micro"
- "usec"
- "us"
- "NanoSecond"
- "nanosecond"
- "nano_second"
- "nano"
- "nsec"
- "ns"

## Example

```yaml
in:
  type: file
  path_prefix: "items"
  parser:
    type: parquet
    columns:
      - {name: "id", type: "long"}
      - {name: "code", type: "string"}
      - {name: "name", type: "string"}
      - {name: "description", type: "string"}
      - {name: "flag", type: "boolean"}
      - {name: "price", type: "long"}
      - {name: "item_type", type: "string"}
      - {name: "tags", type: "json"}
      - {name: "options", type: "json"}
      - {name: "spec", type: "json"}
      - {name: "created_at", type: "timestamp", format: "%Y-%m-%dT%H:%M:%S%:z"}
      - {name: "created_at_utc", type: "timestamp", timestamp_unit: "second"}

out:
  type: stdout
```


You don't have to write `parser:` section in the configuration file. After writing `in:` section, you can let embulk guess `parser:` section using this command:

```
$ embulk gem install embulk-parser-parquet
$ embulk guess -g parquet example/seed.yml -o config.yml
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
