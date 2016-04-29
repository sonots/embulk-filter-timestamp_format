package org.embulk.filter.timestamp_format.cast;

import org.embulk.filter.timestamp_format.TimestampFormatter;
import org.embulk.spi.DataException;
import org.embulk.spi.time.Timestamp;

public class LongCast
{
    private LongCast() {}

    public static String asString(long value, TimestampFormatter formatter) throws DataException
    {
        Timestamp timestamp = Timestamp.ofEpochSecond(value);
        return formatter.format(timestamp);
    }

    public static Timestamp asTimestamp(long value) throws DataException
    {
        return Timestamp.ofEpochSecond(value);
    }

    public static long asLong(long value) throws DataException
    {
        return value;
    }

    public static double asDouble(long value) throws DataException
    {
        return (double) value;
    }
}
