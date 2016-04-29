package org.embulk.filter.timestamp_format.cast;

import org.embulk.filter.timestamp_format.TimestampFormatter;
import org.embulk.spi.DataException;
import org.embulk.spi.time.Timestamp;

public class DoubleCast
{
    private DoubleCast() {}

    public static String asString(double value, TimestampFormatter formatter) throws DataException
    {
        Timestamp timestamp = asTimestamp(value);
        return formatter.format(timestamp);
    }

    public static Timestamp asTimestamp(double value) throws DataException
    {
        long epochSecond = (long) value;
        long nano = (long) ((value - epochSecond) * 1000000000);
        return Timestamp.ofEpochSecond(epochSecond, nano);
    }

    public static long asLong(double value) throws DataException
    {
        return (long) value;
    }

    public static double asDouble(double value) throws DataException
    {
        return value;
    }
}
