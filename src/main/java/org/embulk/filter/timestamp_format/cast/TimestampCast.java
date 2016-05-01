package org.embulk.filter.timestamp_format.cast;

import org.embulk.filter.timestamp_format.TimestampFormatter;
import org.embulk.filter.timestamp_format.TimestampUnit;
import org.embulk.spi.DataException;
import org.embulk.spi.time.Timestamp;

public class TimestampCast
{
    private TimestampCast() {}

    public static String asString(Timestamp value, TimestampFormatter formatter) throws DataException
    {
        return formatter.format(value);
    }

    public static Timestamp asTimestamp(Timestamp value) throws DataException
    {
        return value;
    }

    public static long asLong(Timestamp value, TimestampUnit toUnit) throws DataException
    {
        return TimestampUnit.toLong(value, toUnit);
    }

    public static double asDouble(Timestamp value, TimestampUnit toUnit) throws DataException
    {
        return TimestampUnit.toDouble(value, toUnit);
    }
}
