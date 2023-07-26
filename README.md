# Parquet parser plugin for Embulk

Parquet parser plugin for Embulk.

## Overview

* **Plugin type**: parser
* **Guess supported**: yes

## Configuration

- **type**: Specify this parser as parquet
- **columns**: Specify column name and type. See below (array, optional)
  - timestamp_unit: Specify unit of time. (This config is effective only if parquet value is `long`, `int`, `float`, `double`)
* **default_timezone**: Default timezone of the timestamp (string, default: UTC)
* **default_timestamp_format**: Default timestamp format of the timestamp (string, default: `%Y-%m-%d %H:%M:%S.%N %z`)

If columns is not set, this plugin detect schema automatically by using first record schema.

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
