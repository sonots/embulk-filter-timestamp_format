package org.embulk.filter.timestamp_format;

import org.embulk.spi.time.Timestamp;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestTimestampUnit
{
    @Test
    public void testLongToTimestamp()
    {
        long epochSecond = 1462087147L;
        long epochNanoSecond  = 1462087147100200300L;
        Timestamp timestamp;

        timestamp = TimestampUnit.toTimestamp(epochNanoSecond / 1000000000, TimestampUnit.Second);
        assertEquals(epochSecond, timestamp.getEpochSecond());
        assertEquals(0, timestamp.getNano());

        timestamp = TimestampUnit.toTimestamp(epochNanoSecond / 1000000, TimestampUnit.MilliSecond);
        assertEquals(epochSecond, timestamp.getEpochSecond());
        assertEquals(100000000, timestamp.getNano());

        timestamp = TimestampUnit.toTimestamp(epochNanoSecond / 1000, TimestampUnit.MicroSecond);
        assertEquals(epochSecond, timestamp.getEpochSecond());
        assertEquals(100200000, timestamp.getNano());

        timestamp = TimestampUnit.toTimestamp(epochNanoSecond, TimestampUnit.NanoSecond);
        assertEquals(epochSecond, timestamp.getEpochSecond());
        assertEquals(100200300, timestamp.getNano());
    }

    @Test
    public void testDoubleToTimestamp()
    {
        long epochSecond = 1462087147L;
        double epochNanoSecond  = 1462087147100200192.0;
        Timestamp timestamp;

        timestamp = TimestampUnit.toTimestamp(epochNanoSecond / 1000000000, TimestampUnit.Second);
        assertEquals(epochSecond, timestamp.getEpochSecond());
        assertEquals(100200192, timestamp.getNano(), 200);

        timestamp = TimestampUnit.toTimestamp(epochNanoSecond / 1000000, TimestampUnit.MilliSecond);
        assertEquals(epochSecond, timestamp.getEpochSecond());
        assertEquals(100200192, timestamp.getNano(), 200);

        timestamp = TimestampUnit.toTimestamp(epochNanoSecond / 1000, TimestampUnit.MicroSecond);
        assertEquals(epochSecond, timestamp.getEpochSecond());
        assertEquals(100200192, timestamp.getNano(), 200);

        timestamp = TimestampUnit.toTimestamp(epochNanoSecond, TimestampUnit.NanoSecond);
        assertEquals(epochSecond, timestamp.getEpochSecond());
        assertEquals(100200192, timestamp.getNano());
    }

    @Test
    public void testTimestampToLong()
    {
        long epochNanoSecond = 1462087147100200300L;
        Timestamp timestamp = Timestamp.ofEpochSecond(0, epochNanoSecond);
        long value;

        value = TimestampUnit.toLong(timestamp, TimestampUnit.Second);
        assertEquals(epochNanoSecond / 1000000000, value);

        value = TimestampUnit.toLong(timestamp, TimestampUnit.MilliSecond);
        assertEquals(epochNanoSecond / 1000000, value);

        value = TimestampUnit.toLong(timestamp, TimestampUnit.MicroSecond);
        assertEquals(epochNanoSecond / 1000, value);

        value = TimestampUnit.toLong(timestamp, TimestampUnit.NanoSecond);
        assertEquals(epochNanoSecond, value);
    }

    @Test
    public void testTimestampToDouble()
    {
        long epochNanoSecond = 1462087147100200192L;
        Timestamp timestamp = Timestamp.ofEpochSecond(0, epochNanoSecond);
        double value;

        value = TimestampUnit.toDouble(timestamp, TimestampUnit.Second);
        assertEquals(epochNanoSecond / 1000000000.0, value, 2);

        value = TimestampUnit.toDouble(timestamp, TimestampUnit.MilliSecond);
        assertEquals(epochNanoSecond / 1000000.0, value, 2);

        value = TimestampUnit.toDouble(timestamp, TimestampUnit.MicroSecond);
        assertEquals(epochNanoSecond / 1000.0, value, 2);

        value = TimestampUnit.toDouble(timestamp, TimestampUnit.NanoSecond);
        assertEquals(epochNanoSecond / 1.0, value, 2);
    }

    @Test
    public void testLongChangeUnit()
    {
        long epochNanoSecond = 1462087147100200300L;
        long value;

        // from second
        value = TimestampUnit.changeUnit(epochNanoSecond / 1000000000, TimestampUnit.Second, TimestampUnit.Second);
        assertEquals(epochNanoSecond / 1000000000, value);
        value = TimestampUnit.changeUnit(epochNanoSecond / 1000000000, TimestampUnit.Second, TimestampUnit.MilliSecond);
        assertEquals(epochNanoSecond / 1000000000 * 1000, value);
        value = TimestampUnit.changeUnit(epochNanoSecond / 1000000000, TimestampUnit.Second, TimestampUnit.MicroSecond);
        assertEquals(epochNanoSecond / 1000000000 * 1000000, value);
        value = TimestampUnit.changeUnit(epochNanoSecond / 1000000000, TimestampUnit.Second, TimestampUnit.NanoSecond);
        assertEquals(epochNanoSecond / 1000000000 * 1000000000, value);

        // from milli second
        value = TimestampUnit.changeUnit(epochNanoSecond / 1000000, TimestampUnit.MilliSecond, TimestampUnit.Second);
        assertEquals(epochNanoSecond / 1000000 / 1000, value);
        value = TimestampUnit.changeUnit(epochNanoSecond / 1000000, TimestampUnit.MilliSecond, TimestampUnit.MilliSecond);
        assertEquals(epochNanoSecond / 1000000, value);
        value = TimestampUnit.changeUnit(epochNanoSecond / 1000000, TimestampUnit.MilliSecond, TimestampUnit.MicroSecond);
        assertEquals(epochNanoSecond / 1000000 * 1000, value);
        value = TimestampUnit.changeUnit(epochNanoSecond / 1000000, TimestampUnit.MilliSecond, TimestampUnit.NanoSecond);
        assertEquals(epochNanoSecond / 1000000 * 1000000, value);

        // from micro second
        value = TimestampUnit.changeUnit(epochNanoSecond / 1000, TimestampUnit.MicroSecond, TimestampUnit.Second);
        assertEquals(epochNanoSecond / 1000 / 1000000, value);
        value = TimestampUnit.changeUnit(epochNanoSecond / 1000, TimestampUnit.MicroSecond, TimestampUnit.MilliSecond);
        assertEquals(epochNanoSecond / 1000 / 1000, value);
        value = TimestampUnit.changeUnit(epochNanoSecond / 1000, TimestampUnit.MicroSecond, TimestampUnit.MicroSecond);
        assertEquals(epochNanoSecond / 1000, value);
        value = TimestampUnit.changeUnit(epochNanoSecond / 1000, TimestampUnit.MicroSecond, TimestampUnit.NanoSecond);
        assertEquals(epochNanoSecond / 1000 * 1000, value);

        // from nano second
        value = TimestampUnit.changeUnit(epochNanoSecond, TimestampUnit.NanoSecond, TimestampUnit.Second);
        assertEquals(epochNanoSecond / 1000000000, value);
        value = TimestampUnit.changeUnit(epochNanoSecond, TimestampUnit.NanoSecond, TimestampUnit.MilliSecond);
        assertEquals(epochNanoSecond / 1000000, value);
        value = TimestampUnit.changeUnit(epochNanoSecond, TimestampUnit.NanoSecond, TimestampUnit.MicroSecond);
        assertEquals(epochNanoSecond / 1000, value);
        value = TimestampUnit.changeUnit(epochNanoSecond, TimestampUnit.NanoSecond, TimestampUnit.NanoSecond);
        assertEquals(epochNanoSecond, value);
    }

    @Test
    public void testDoubleChangeUnit()
    {
        double epochNanoSecond = 1462087147100200192L;
        double value;

        // from second
        value = TimestampUnit.changeUnit(epochNanoSecond / 1000000000, TimestampUnit.Second, TimestampUnit.Second);
        assertEquals(epochNanoSecond / 1000000000, value, 2);
        value = TimestampUnit.changeUnit(epochNanoSecond / 1000000000, TimestampUnit.Second, TimestampUnit.MilliSecond);
        assertEquals(epochNanoSecond / 1000000000 * 1000, value, 2);
        value = TimestampUnit.changeUnit(epochNanoSecond / 1000000000, TimestampUnit.Second, TimestampUnit.MicroSecond);
        assertEquals(epochNanoSecond / 1000000000 * 1000000, value, 2);
        value = TimestampUnit.changeUnit(epochNanoSecond / 1000000000, TimestampUnit.Second, TimestampUnit.NanoSecond);
        assertEquals(epochNanoSecond / 1000000000 * 1000000000, value, 2);

        // from milli second
        value = TimestampUnit.changeUnit(epochNanoSecond / 1000000, TimestampUnit.MilliSecond, TimestampUnit.Second);
        assertEquals(epochNanoSecond / 1000000 / 1000, value ,2);
        value = TimestampUnit.changeUnit(epochNanoSecond / 1000000, TimestampUnit.MilliSecond, TimestampUnit.MilliSecond);
        assertEquals(epochNanoSecond / 1000000, value, 2);
        value = TimestampUnit.changeUnit(epochNanoSecond / 1000000, TimestampUnit.MilliSecond, TimestampUnit.MicroSecond);
        assertEquals(epochNanoSecond / 1000000 * 1000, value, 2);
        value = TimestampUnit.changeUnit(epochNanoSecond / 1000000, TimestampUnit.MilliSecond, TimestampUnit.NanoSecond);
        assertEquals(epochNanoSecond / 1000000 * 1000000, value, 2);

        // from micro second
        value = TimestampUnit.changeUnit(epochNanoSecond / 1000, TimestampUnit.MicroSecond, TimestampUnit.Second);
        assertEquals(epochNanoSecond / 1000 / 1000000, value, 2);
        value = TimestampUnit.changeUnit(epochNanoSecond / 1000, TimestampUnit.MicroSecond, TimestampUnit.MilliSecond);
        assertEquals(epochNanoSecond / 1000 / 1000, value, 2);
        value = TimestampUnit.changeUnit(epochNanoSecond / 1000, TimestampUnit.MicroSecond, TimestampUnit.MicroSecond);
        assertEquals(epochNanoSecond / 1000, value, 2);
        value = TimestampUnit.changeUnit(epochNanoSecond / 1000, TimestampUnit.MicroSecond, TimestampUnit.NanoSecond);
        assertEquals(epochNanoSecond / 1000 * 1000, value, 2);

        // from nano second
        value = TimestampUnit.changeUnit(epochNanoSecond, TimestampUnit.NanoSecond, TimestampUnit.Second);
        assertEquals(epochNanoSecond / 1000000000, value, 2);
        value = TimestampUnit.changeUnit(epochNanoSecond, TimestampUnit.NanoSecond, TimestampUnit.MilliSecond);
        assertEquals(epochNanoSecond / 1000000, value, 2);
        value = TimestampUnit.changeUnit(epochNanoSecond, TimestampUnit.NanoSecond, TimestampUnit.MicroSecond);
        assertEquals(epochNanoSecond / 1000, value, 2);
        value = TimestampUnit.changeUnit(epochNanoSecond, TimestampUnit.NanoSecond, TimestampUnit.NanoSecond);
        assertEquals(epochNanoSecond, value, 2);
    }

}
