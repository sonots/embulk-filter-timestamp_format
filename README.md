# Timestamp format filter plugin for Embulk

[![Build Status](https://secure.travis-ci.org/sonots/embulk-filter-timestamp_format.png?branch=master)](http://travis-ci.org/sonots/embulk-filter-timestamp_format)

A filter plugin for Embulk to change timesatmp format

## Configuration

- **columns**: columns to retain (array of hash)
  - **name**: name of column, must be a string column (required)
  - **from_format**: specify the format of the input timestamp (array of strings, default is default_from_format)
  - **from_timezone**: specify the timezone of the input timestamp (string, default is default_from_timezone)
  - **to_format**: specify the format of the output timestamp (string, default is default_to_format)
  - **to_timezone**: specify the timezone of the output timestamp (string, default is default_to_timezone)
- **default_from_format**: default timestamp format for the input timestamp columns (array of strings, default is `["%Y-%m-%d %H:%M:%S.%N %z"]`)
- **default_from_timezone**: default timezone for the input timestamp columns (string, default is `UTC`)
- **default_to_format**: default timestamp format for the output timestamp columns (string, default is `%Y-%m-%d %H:%M:%S.%N %z`)
- **default_to_timezone**: default timezone for the output timestamp olumns (string, default is `UTC`)

## Example

Say example.jsonl is as follows (this is a typical format which Exporting BigQuery table outputs):

```
{"timestamp":"2015-07-12 15:00:00 UTC","record":{"timestamp":"2015-07-12 15:00:00 UTC"}}
{"timestamp":"2015-07-12 15:00:00.1 UTC","record":{"timestamp":"2015-07-12 15:00:00.1 UTC"}}
```

```yaml
in:
  type: file
  path_prefix: example/example.jsonl
  parser:
    type: jsonl
    columns:
    - {name: timestamp, type: string}
    - {name: record, type: json}
filters:
  - type: timestamp_format
    default_to_timezone: "Asia/Tokyo"
    default_to_format: "%Y-%m-%d %H:%M:%S.%N"
    columns:
      - {name: timestamp, from_format: ["%Y-%m-%d %H:%M:%S.%N %z", "%Y-%m-%d %H:%M:%S %z"]}
      - {name: record.timestamp, from_format: ["%Y-%m-%d %H:%M:%S.%N %z", "%Y-%m-%d %H:%M:%S %z"]}
  type: stdout
```

Output will be as:

```
{"timestamp":"2015-07-13 00:00:00.0","record":{"timestamp":"2015-07-13 00:00:00.0}}
{"timestamp":"2015-07-13 00:00:00.1","record":{"timestamp":"2015-07-13 00:00:00.1}}
```

## ToDo

* Currently, input must be a String column and output will be a String column. But, support Timestamp column (input / output)
* Write test

## Development

Run example:

```
$ embulk gem install embulk-parser-jsonl
$ ./gradlew classpath
$ embulk run -I lib example/example.yml
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
