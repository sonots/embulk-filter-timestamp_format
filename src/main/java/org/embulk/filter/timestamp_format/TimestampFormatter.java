package org.embulk.filter.timestamp_format;

import com.google.common.base.Optional;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;

import org.embulk.filter.timestamp_format.TimestampFormatFilterPlugin.PluginTask;

import org.embulk.spi.time.Timestamp;
import org.embulk.spi.util.LineEncoder;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.jruby.embed.ScriptingContainer;
import org.jruby.util.RubyDateFormat;

import java.util.Locale;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

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

    private final RubyDateFormat jrubyFormatter;
    private final DateTimeFormatter javaFormatter;
    private boolean handleNanoResolution = false;
    private boolean handleMicroResolution = false;
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
        if (format.contains("%")) {
            this.javaFormatter = null;
            this.jrubyFormatter = new RubyDateFormat(format, Locale.ENGLISH, true);
        }
        else {
            this.jrubyFormatter = null;
            if (format.contains("nnnnnnnnn")) {
                this.handleNanoResolution = true;
                String newFormat = format.replaceAll("nnnnnnnnn", "'%09d'");
                this.javaFormatter = DateTimeFormat.forPattern(newFormat).withLocale(Locale.ENGLISH).withZone(toTimeZone);
            }
            else if (format.contains("nnnnnn")) {
                this.handleMicroResolution = true;
                String newFormat = format.replaceAll("nnnnnn", "'%06d'");
                this.javaFormatter = DateTimeFormat.forPattern(newFormat).withLocale(Locale.ENGLISH).withZone(toTimeZone);
            }
            else {
                this.javaFormatter = DateTimeFormat.forPattern(format).withLocale(Locale.ENGLISH).withZone(toTimeZone);
            }
        }
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
        if (jrubyFormatter != null) {
            return jrubyFormat(value);
        }
        else if (javaFormatter != null) {
            return javaFormat(value);
        }
        else {
            assert false;
            throw new RuntimeException();
        }
    }

    private String jrubyFormat(Timestamp value)
    {
        // TODO optimize by using reused StringBuilder
        jrubyFormatter.setDateTime(new DateTime(value.getEpochSecond() * 1000, toTimeZone));
        jrubyFormatter.setNSec(value.getNano());
        return jrubyFormatter.format(null);
    }

    private String javaFormat(Timestamp value)
    {
        if (handleNanoResolution) {
            String datetimeFormatted = javaFormatter.print(value.getEpochSecond() * 1000);
            return String.format(datetimeFormatted, value.getNano());
        }
        else if (handleMicroResolution) {
            String datetimeFormatted = javaFormatter.print(value.getEpochSecond() * 1000);
            return String.format(datetimeFormatted, value.getNano() / 1000);
        }
        else {
            long milliSecond = value.getEpochSecond() * 1000 + value.getNano() / 1000000;
            return javaFormatter.print(milliSecond);
        }
    }
}
