package org.embulk.filter.timestamp_format.cast;

import org.embulk.filter.timestamp_format.TimestampFormatter;
import org.embulk.filter.timestamp_format.TimestampParser;
import org.embulk.spi.DataException;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParseException;

public class StringCast
{
    private StringCast() {}

    private static String buildErrorMessage(String value)
    {
        return String.format("failed to parse string: \"%s\"", value);
    }

    public static String asString(String value, TimestampParser parser, TimestampFormatter formatter) throws DataException
    {
        try {
            Timestamp timestamp = parser.parse(value);
            return formatter.format(timestamp);
        }
        catch (TimestampParseException ex) {
            throw new DataException(buildErrorMessage(value), ex);
        }
    }

    public static Timestamp asTimestamp(String value, TimestampParser parser) throws DataException
    {
        try {
            return parser.parse(value);
        }
        catch (TimestampParseException ex) {
            throw new DataException(buildErrorMessage(value), ex);
        }
    }

    public static long asLong(String value, TimestampParser parser) throws DataException
    {
        try {
            Timestamp timestamp = parser.parse(value);
            return timestamp.getEpochSecond();
        }
        catch (TimestampParseException ex) {
            throw new DataException(buildErrorMessage(value), ex);
        }
    }
    public static double asDouble(String value, TimestampParser parser) throws DataException
    {
        try {
            Timestamp timestamp = parser.parse(value);
            return TimestampCast.asDouble(timestamp);
        }
        catch (TimestampParseException ex) {
            throw new DataException(buildErrorMessage(value), ex);
        }
    }
}
