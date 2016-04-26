package org.embulk.filter.timestamp_format;

import java.util.Locale;
import org.jruby.embed.ScriptingContainer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import com.google.common.base.Optional;
import org.jruby.embed.ScriptingContainer;
import org.jruby.util.RubyDateFormat;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.spi.util.LineEncoder;
import org.embulk.spi.time.Timestamp;

import org.embulk.filter.TimestampFormatFilterPlugin.PluginTask;

public class TimestampFormatter
{
    public interface Task
    {
        @Config("default_to_timezone")
        @ConfigDefault("\"UTC\"")
        DateTimeZone getDefaultToTimeZone();

        @Config("default_to_timestamp_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%6N %z\"")
        String getDefaultToTimestampFormat();
    }

    public interface TimestampColumnOption
    {
        @Config("to_timezone")
        @ConfigDefault("null")
        Optional<DateTimeZone> getToTimeZone();

        @Config("to_format")
        @ConfigDefault("null")
        Optional<String> getToFormat();
    }

    private final RubyDateFormat toDateFormat;
    private final DateTimeZone toTimeZone;

    public TimestampFormatter(PluginTask task, Optional<? extends TimestampColumnOption> columnOption)
    {
        this(task.getJRuby(),
                columnOption.isPresent() ?
                    columnOption.get().getToFormat().or(task.getDefaultToTimestampFormat())
                    : task.getDefaultToTimestampFormat(),
                columnOption.isPresent() ?
                    columnOption.get().getToTimeZone().or(task.getDefaultToTimeZone())
                    : task.getDefaultToTimeZone());
    }

    public TimestampFormatter(ScriptingContainer jruby, String format, DateTimeZone toTimeZone)
    {
        this.toTimeZone = toTimeZone;
        this.toDateFormat = new RubyDateFormat(format, Locale.ENGLISH, true);
    }

    public DateTimeZone getToTimeZone()
    {
        return toTimeZone;
    }

    public void format(Timestamp value, LineEncoder encoder)
    {
        // TODO optimize by directly appending to internal buffer
        encoder.addText(format(value));
    }

    public String format(Timestamp value)
    {
        // TODO optimize by using reused StringBuilder
        toDateFormat.setDateTime(new DateTime(value.getEpochSecond()*1000, toTimeZone));
        toDateFormat.setNSec(value.getNano());
        return toDateFormat.format(null);
    }
}
