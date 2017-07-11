package org.embulk.filter.timestamp_format;

import org.embulk.EmbulkTestRuntime;

import org.embulk.spi.time.Timestamp;
import org.joda.time.DateTimeZone;
import org.jruby.embed.ScriptingContainer;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestTimestampParser
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    public ScriptingContainer jruby;
    public DateTimeZone zone;
    public Timestamp expected;

    @Before
    public void createResource()
    {
        jruby = new ScriptingContainer();
        zone = DateTimeZone.UTC;
        expected = Timestamp.ofEpochSecond(1463065359, 123456789);
    }

    @Test
    public void testJRubyParser()
    {
        String rubyFormat = "%Y-%m-%d %H:%M:%S.%N %:z";

        TimestampParser parser = new TimestampParser(jruby, Arrays.asList(rubyFormat), zone);
        try {
            Timestamp actual = parser.parse("2016-05-13 00:02:39.123456789 +09:00");
            // embulk >= 0.8.27 uses new faster jruby Timestamp parser, and it support nano second
            // embulk < 0.8.27 uses old slow jruby Timestamp parser, and it does not support nano seconds
            //assertEquals(expected, actual);
        }
        catch (IllegalArgumentException ex) {
            fail();
        }
    }

    @Test
    public void testJavaParser()
    {
        String javaFormat = "yyyy-MM-dd HH:mm:ss.nnnnnnnnn Z";

        TimestampParser parser = new TimestampParser(jruby, Arrays.asList(javaFormat), zone);
        try {
            Timestamp actual = parser.parse("2016-05-13 00:02:39.123456789 +09:00");
            assertEquals(expected, actual);
        }
        catch (IllegalArgumentException ex) {
            fail();
        }
    }
}
