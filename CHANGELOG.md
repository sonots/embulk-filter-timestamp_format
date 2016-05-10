# 0.1.8 (2016-05-10)

Enhancements:

* Support nano second resolution for Java parser

# 0.1.7 (2016-05-09)

Enhancements:

* Use Joda-Time DateTimeFormat instead of SimpleDateFormat for Java timestamp parser/formatter
  * to be thread-safe
  * to fix ss.SSS resolves 1.1 as 1.001 seconds wrongly

# 0.1.6 (2016-05-01)

Enhancements:

* Support unixtimestamp unit such as milli sec, micro sec, nano sec
* Support Java timestamp parser/formatter (SimpleDateFormat)

# 0.1.5 (2016-04-29)

Enhancements:

* Support to cast from/into timestamp
* Support to cast into long/double (unixtimesatmp)

# 0.1.4 (2016-04-26)

Enhancements:

* Performance Improvement by avoiding unnecessary visiting

# 0.1.3 (2016-04-26)

Fixes:

* Fix to see all from_format

# 0.1.2 (2016-04-26)

Changes:

* Relax ConfigException

# 0.1.1 (2016-04-26)

Enhancements:

* Check whether specified columns exist

# 0.1.0 (2016-04-26)

initial version
