# Timestamp format filter plugin for Embulk

[![Build Status](https://secure.travis-ci.org/sonots/embulk-filter-timestamp_format.png?branch=master)](http://travis-ci.org/sonots/embulk-filter-timestamp_format)

A filter plugin for Embulk to change timestamp format

## Configuration

- **columns**: columns to retain (array of hash)
  - **name**: name of column (required)
  - **type**: type to cast (string, timestamp, long (unixtimestamp), double (unixtimestamp), default is string)
  - **from_format**: specify the format of the input string (array of strings, default is default_from_timestamp_format)
  - **from_timezone**: specify the timezone of the input string (string, default is default_from_timezone)
  - **to_format**: specify the format of the output string (string, default is default_to_timestamp_format)
  - **to_timezone**: specify the timezone of the output string (string, default is default_to_timezone)
- **default_from_timestamp_format**: default timestamp format for the input string (array of strings, default is `["%Y-%m-%d %H:%M:%S.%N %z"]`)
- **default_from_timezone**: default timezone for the input string (string, default is `UTC`)
- **default_to_timestamp_format**: default timestamp format for the output string (string, default is `%Y-%m-%d %H:%M:%S.%N %z`)
- **default_to_timezone**: default timezone for the output string (string, default is `UTC`)
* **stop_on_invalid_record**: stop bulk load transaction if a invalid record is found (boolean, default is `false)

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
    type: jsonl
    columns:
    - {name: timestamp, type: string}
    - {name: nested, type: json}
filters:
  - type: timestamp_format
    default_to_timezone: "Asia/Tokyo"
    default_to_timestamp_format: "%Y-%m-%d %H:%M:%S.%N"
    columns:
      - {name: timestamp, from_format: ["%Y-%m-%d %H:%M:%S.%N %z", "%Y-%m-%d %H:%M:%S %z"]}
      - {name: $.nested.timestamp, from_format: ["%Y-%m-%d %H:%M:%S.%N %z", "%Y-%m-%d %H:%M:%S %z"]}
  type: stdout
```

Output will be as:

```
{"timestamp":"2015-07-13 00:00:00.0","nested":{"timestamp":"2015-07-13 00:00:00.0}}
{"timestamp":"2015-07-13 00:00:00.1","nested":{"timestamp":"2015-07-13 00:00:00.1}}
```

See [./example](./example) for more examples.

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
