package org.embulk.filter.timestamp_format.cast;

import org.embulk.filter.timestamp_format.TimestampFormatter;
import org.embulk.filter.timestamp_format.TimestampUnit;
import org.embulk.spi.DataException;
import org.embulk.spi.time.Timestamp;

public class DoubleCast
{
    private DoubleCast() {}

    public static String asString(double value, TimestampUnit fromUnit, TimestampFormatter formatter) throws DataException
    {
        Timestamp timestamp = TimestampUnit.asTimestamp(value, fromUnit);
        return formatter.format(timestamp);
    }

    public static Timestamp asTimestamp(double value, TimestampUnit fromUnit) throws DataException
    {
        return TimestampUnit.asTimestamp(value, fromUnit);
    }

    public static long asLong(double value, TimestampUnit fromUnit, TimestampUnit toUnit) throws DataException
    {
        return (long) TimestampUnit.changeUnit(value, fromUnit, toUnit);
    }

    public static double asDouble(double value, TimestampUnit fromUnit, TimestampUnit toUnit) throws DataException
    {
        return TimestampUnit.changeUnit(value, fromUnit, toUnit);
    }
}
