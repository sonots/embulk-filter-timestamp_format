package org.embulk.filter.timestamp_format;

import org.embulk.EmbulkTestRuntime;

import org.embulk.spi.time.Timestamp;
import org.joda.time.DateTimeZone;
import org.jruby.embed.ScriptingContainer;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestTimestampFormatConverter
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    public ScriptingContainer jruby;
    public DateTimeZone zone;
    public Timestamp timestamp;

    @Before
    public void createResource()
    {
        jruby = new ScriptingContainer();
        zone = DateTimeZone.UTC;
        timestamp = Timestamp.ofEpochSecond(1463130159, 123456789);
    }

    @Test
    public void testRUBY_TO_JAVA_FORMAT_TABLE()
    {
        for(Map.Entry<String, String> entry : TimestampFormatConverter.RUBY_TO_JAVA_FORMAT_TABLE.entrySet()) {
            String rubyFormat = "%" + entry.getKey();
            String javaFormat = entry.getValue();

            TimestampFormatter rubyFormatter = new TimestampFormatter(jruby, rubyFormat, zone);
            TimestampFormatter javaFormatter = new TimestampFormatter(jruby, javaFormat, zone);
            String rubyFormatted = rubyFormatter.format(timestamp);
            String javaFormatted = javaFormatter.format(timestamp);
            // System.out.println(String.format("%s<%s> %s<%s>", rubyFormat, rubyFormatted, javaFormat, javaFormatted));

            TimestampParser rubyParser = new TimestampParser(jruby, Arrays.asList("." + rubyFormat), zone);
            TimestampParser javaParser = new TimestampParser(jruby, Arrays.asList("." + javaFormat), zone);
            Timestamp rubyParsed = rubyParser.parse("." + rubyFormatted);
            try {
                Timestamp javaParsed = javaParser.parse("." + rubyFormatted);
            }
            catch (IllegalArgumentException ex) {
                fail(String.format("Parse \"%s\" with java format \"%s\" failed (corresponding ruby format \"%s\")", rubyFormatted, javaFormat, rubyFormat));
            }
        }
    }

    @Test
    public void testToJavaFormats()
    {
        for(Map.Entry<String, String> entry : TimestampFormatConverter.RUBY_TO_JAVA_FORMAT_TABLE.entrySet()) {
            String rubyFormat = "%-2" + entry.getKey();
            String javaFormat = entry.getValue();
            assertEquals(javaFormat, TimestampFormatConverter.toJavaFormat(rubyFormat));
        }
    }

    @Test
    public void testToJavaFormat()
    {
        {
            String rubyFormat = "%Y-%m-%d %H:%M:%S.%6N %:z";
            String javaFormat = "yyyy-MM-dd HH:mm:ss.nnnnnnnnn Z";
            assertEquals(javaFormat, TimestampFormatConverter.toJavaFormat(rubyFormat));

            TimestampParser parser = new TimestampParser(jruby, Arrays.asList(javaFormat), zone);
            try {
                parser.parse("2016-05-12 20:14:13.123456789 +09:00");
            }
            catch (IllegalArgumentException ex) {
                fail();
            }
        }
        {
            String rubyFormat = "%Y-%m-%d %H:%M:%S.%6N UTC";
            String javaFormat = "yyyy-MM-dd HH:mm:ss.nnnnnnnnn 'UTC'";
            assertEquals(javaFormat, TimestampFormatConverter.toJavaFormat(rubyFormat));

            TimestampParser parser = new TimestampParser(jruby, Arrays.asList(javaFormat), zone);
            try {
                parser.parse("2016-05-12 20:14:13.123456789 UTC");
            }
            catch (IllegalArgumentException ex) {
                fail();
            }
        }
        {
            String rubyFormat = "%Y-%m-%d %H:%M:%S.%6N +00:00";
            String javaFormat = "yyyy-MM-dd HH:mm:ss.nnnnnnnnn '+00:00'";
            assertEquals(javaFormat, TimestampFormatConverter.toJavaFormat(rubyFormat));

            TimestampParser parser = new TimestampParser(jruby, Arrays.asList(javaFormat), zone);
            try {
                parser.parse("2016-05-12 20:14:13.123456789 +00:00");
            }
            catch (IllegalArgumentException ex) {
                fail();
            }
        }
    }
}

