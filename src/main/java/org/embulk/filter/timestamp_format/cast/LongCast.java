package org.embulk.filter.timestamp_format.cast;

import org.embulk.filter.timestamp_format.TimestampFormatter;
import org.embulk.filter.timestamp_format.TimestampUnit;
import org.embulk.spi.DataException;
import org.embulk.spi.time.Timestamp;

public class LongCast
{
    private LongCast() {}

    public static String asString(long value, TimestampUnit fromUnit, TimestampFormatter formatter) throws DataException
    {
        Timestamp timestamp = TimestampUnit.toTimestamp(value, fromUnit);
        return formatter.format(timestamp);
    }

    public static Timestamp asTimestamp(long value, TimestampUnit fromUnit) throws DataException
    {
        return TimestampUnit.toTimestamp(value, fromUnit);
    }

    public static long asLong(long value, TimestampUnit fromUnit, TimestampUnit toUnit) throws DataException
    {
        return TimestampUnit.changeUnit(value, fromUnit, toUnit);
    }

    public static double asDouble(long value, TimestampUnit fromUnit, TimestampUnit toUnit) throws DataException
    {
        return (double) TimestampUnit.changeUnit(value, fromUnit, toUnit);
    }
}
