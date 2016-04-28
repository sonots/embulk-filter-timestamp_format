package org.embulk.filter.timestamp_format.cast;

import org.embulk.filter.timestamp_format.TimestampFormatter;
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

    public static long asLong(Timestamp value) throws DataException
    {
        return value.getEpochSecond();
    }

    public static double asDouble(Timestamp value) throws DataException
    {
        long epoch = value.getEpochSecond();
        int nano = value.getNano();
        return (double) epoch + ((double) nano / 1000000000.0);
    }
}
