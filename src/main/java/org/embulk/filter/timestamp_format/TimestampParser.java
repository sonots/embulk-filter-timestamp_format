package org.embulk.filter.timestamp_format;

import com.google.common.base.Optional;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;

import org.embulk.filter.TimestampFormatFilterPlugin.PluginTask;

import org.embulk.spi.time.JRubyTimeParserHelper;
import org.embulk.spi.time.JRubyTimeParserHelperFactory;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParseException;

import static org.embulk.spi.time.TimestampFormat.parseDateTimeZone;

import org.joda.time.DateTimeZone;
import org.jruby.embed.ScriptingContainer;

import java.util.ArrayList;
import java.util.List;

public class TimestampParser
{
    public interface Task
    {
        @Config("default_from_timezone")
        @ConfigDefault("\"UTC\"")
        DateTimeZone getDefaultFromTimeZone();

        @Config("default_from_timestamp_format")
        @ConfigDefault("[\"%Y-%m-%d %H:%M:%S.%N %z\"]")
        List<String> getDefaultFromTimestampFormat();
    }

    public interface TimestampColumnOption
    {
        @Config("from_timezone")
        @ConfigDefault("null")
        Optional<DateTimeZone> getFromTimeZone();

        @Config("from_format")
        @ConfigDefault("null")
        Optional<List<String>> getFromFormat();
    }

    private final List<JRubyTimeParserHelper> helperList;
    private final DateTimeZone defaultFromTimeZone;

    TimestampParser(PluginTask task)
    {
        this(task.getJRuby(), task.getDefaultFromTimestampFormat(), task.getDefaultFromTimeZone());
    }

    public TimestampParser(PluginTask task, TimestampColumnOption columnOption)
    {
        this(task.getJRuby(),
                columnOption.getFromFormat().or(task.getDefaultFromTimestampFormat()),
                columnOption.getFromTimeZone().or(task.getDefaultFromTimeZone()));
    }

    public TimestampParser(ScriptingContainer jruby, List<String> formatList, DateTimeZone defaultFromTimeZone)
    {
        JRubyTimeParserHelperFactory helperFactory = (JRubyTimeParserHelperFactory) jruby.runScriptlet("Embulk::Java::TimeParserHelper::Factory.new");
        // TODO get default current time from ExecTask.getExecTimestamp
        this.helperList = new ArrayList<JRubyTimeParserHelper>();
        for (String format : formatList) {
            JRubyTimeParserHelper helper = (JRubyTimeParserHelper) helperFactory.newInstance(format, 1970, 1, 1, 0, 0, 0, 0);  // TODO default time zone
            this.helperList.add(helper);
        }
        this.defaultFromTimeZone = defaultFromTimeZone;
    }

    public DateTimeZone getDefaultFromTimeZone()
    {
        return defaultFromTimeZone;
    }

    public Timestamp parse(String text) throws TimestampParseException
    {
        JRubyTimeParserHelper helper = null;
        long localUsec = -1;
        TimestampParseException exception = null;

        for (JRubyTimeParserHelper h : helperList) {
            helper = h;
            try {
                localUsec = helper.strptimeUsec(text);
                break;
            }
            catch (TimestampParseException ex) {
                exception = ex;
            }
        }
        if (localUsec == -1) {
            throw exception;
        }
        String zone = helper.getZone();

        DateTimeZone timeZone = defaultFromTimeZone;
        if (zone != null) {
            // TODO cache parsed zone?
            timeZone = parseDateTimeZone(zone);
            if (timeZone == null) {
                throw new TimestampParseException("Invalid time zone name '" + text + "'");
            }
        }

        long localSec = localUsec / 1000000;
        long usec = localUsec % 1000000;
        long sec = timeZone.convertLocalToUTC(localSec * 1000, false) / 1000;

        return Timestamp.ofEpochSecond(sec, usec * 1000);
    }
}
