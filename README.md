# Timestamp format filter plugin for Embulk

[![Build Status](https://secure.travis-ci.org/sonots/embulk-filter-timestamp_format.png?branch=master)](http://travis-ci.org/sonots/embulk-filter-timestamp_format)

A filter plugin for Embulk to change timestamp format

## Configuration

- **columns**: columns to retain (array of hash)
  - **name**: name of column (required)
  - **type**: type to cast, choose one of `string`, `timestamp`, `long` (unixtimestamp), `double` (unixtimestamp) (string, default is `string`)
  - **from_format**: specify the format of the input string (array of strings, default is default_from_timestamp_format)
  - **from_timezone**: specify the timezone of the input string (string, default is default_from_timezone)
  - **to_format**: specify the format of the output string (string, default is default_to_timestamp_format)
  - **to_timezone**: specify the timezone of the output string (string, default is default_to_timezone)
  - **from_unit**: specify the time unit of the input unixtimestamp (string, default is default_from_timestamp_unit)
  - **to_unit**: specify the time unit of the output unixtimestamp (string, default is default_to_timestamp_unit)
- **default_from_timestamp_format**: default timestamp format for the input string (array of strings, default is `["%Y-%m-%d %H:%M:%S.%N %z"]`)
- **default_from_timezone**: default timezone for the input string (string, default is `UTC`)
- **default_to_timestamp_format**: default timestamp format for the output string (string, default is `%Y-%m-%d %H:%M:%S.%N %z`)
- **default_to_timezone**: default timezone for the output string (string, default is `UTC`)
- **default_from_timestamp_unit**: default time unit such as `second`, `ms`, `us`, `ns` for the input unixtimestamp (string, default is `second`)
- **default_to_timestamp_unit**: default time unit such as `second`, `ms`, `us`, `ns` for the output unixtimestamp (string, default is `second`)
- **stop_on_invalid_record**: stop bulk load transaction if a invalid record is found (boolean, default is `false`)

## Example

Say example.jsonl is as follows (this is a typical format which Exporting BigQuery table outputs):

```
{"timestamp":"2015-07-12 15:00:00 UTC","nested":{"timestamp":"2015-07-12 15:00:00 UTC"}}
{"timestamp":"2015-07-12 15:00:00.1 UTC","nested":{"timestamp":"2015-07-12 15:00:00.1 UTC"}}
```

```yaml
in:
  type: file
  path_prefix: example/example.jsonl
  parser:
    type: jsonl # not json parser
    columns:
    - {name: timestamp, type: string}
    - {name: nested, type: json}
filters:
  - type: timestamp_format
    default_from_timestamp_format: ["%Y-%m-%d %H:%M:%S.%N %z", "%Y-%m-%d %H:%M:%S %z"]
    default_to_timezone: "Asia/Tokyo"
    default_to_timestamp_format: "%Y-%m-%d %H:%M:%S.%N"
    columns:
      - {name: timestamp, type: long, to_unit: ms}
      - {name: $.nested.timestamp}
out:
  type: stdout
```

Output will be as:

```
{"timestamp":1436713200000,"nested":{"timestamp":"2015-07-13 00:00:00.0}}
{"timestamp":1436713200100,"nested":{"timestamp":"2015-07-13 00:00:00.1}}
```

See [./example](./example) for more examples.

## JRuby Timestamp Parser/Formatter Performance Issue

Embulk's timestamp parser/formatter originally uses jruby implementation, but it is slow.
To improve performance, this plugin also supports Java's Joda-Time [DateTimeFormat](http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html) format as:

```yaml
in:
  type: file
  path_prefix: example/example.jsonl
  parser:
    type: jsonl
    columns:
    - {name: timestamp, type: string}
    - {name: nested, type: json}
filters:
  - type: timestamp_format
    default_from_timestamp_format: ["yyyy-MM-dd HH:mm:ss.SSS z", "yyyy-MM-dd HH:mm:ss z", "yyyy-MM-dd HH:mm:ss"]
    default_to_timezone: "Asia/Taipei"
    default_to_timestamp_format: "yyyy-MM-dd HH:mm:ss.SSS Z"
    columns:
      - {name: timestamp, type: long, to_unit: ms}
      - {name: $.nested.timestamp}
out:
  type: stdout
```

If format strings contain `%`, jruby parser/formatter is used. Otherwirse, java parser/formatter is used

**COMPARISON:**

Benchmark test sets are available at [./bench](./bench).  In my environment (Mac Book Pro), for 1000000 timestamps:

* java parser / java formatter: 1.3s
* java parser / jruby formatter: 1.4s
* jruby parser / java formatter: 64.52s
* jruby parser / jruby formatter: 65.06s

## Nano Resolution

JRuby parser has micro second resolution. Java (Joda-Time) parser has milli second resolution (although Java8's DateTimeFormatter supports nano second resolution)

Nano second resolution is partially supported by this plugin itself. Use parser format `nnnnnnnnn` for Java parser as

```
yyyy-MM-dd HH:mm:ss.nnnnnnnnn z
```

This plugin finds places of nano second from texts with regular expression `\.(\d+)`.

For formatter, you can use jruby formatter as

```
%Y-%m-%d %H:%M:%S.%N %z
```

## ToDo

* Write test

## Development

Run example:

```
$ ./gradlew classpath
$ embulk preview -I lib example/example.yml
```

Run test:

```
$ ./gradlew test
```

Run checkstyle:

```
$ ./gradlew check
```

Release gem:

```
$ ./gradlew gemPush
```
