package org.embulk.filter.timestamp_format;

import org.embulk.filter.timestamp_format.cast.StringCast;
import org.embulk.filter.timestamp_format.cast.TimestampCast;
import org.embulk.filter.timestamp_format.TimestampFormatFilterPlugin.ColumnConfig;
import org.embulk.filter.timestamp_format.TimestampFormatFilterPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;
import org.joda.time.DateTimeZone;
import org.msgpack.value.Value;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;

public class ColumnCaster
{
    private static final Logger logger = Exec.getLogger(TimestampFormatFilterPlugin.class);
    private final PluginTask task;
    private final Schema inputSchema;
    private final Schema outputSchema;
    private final PageReader pageReader;
    private final PageBuilder pageBuilder;
    private final HashMap<String, TimestampParser> timestampParserMap = new HashMap<>();
    private final HashMap<String, TimestampFormatter> timestampFormatterMap = new HashMap<>();
    private final JsonVisitor jsonVisitor;

    ColumnCaster(PluginTask task, Schema inputSchema, Schema outputSchema, PageReader pageReader, PageBuilder pageBuilder)
    {
        this.task         = task;
        this.inputSchema  = inputSchema;
        this.outputSchema = outputSchema;
        this.pageReader   = pageReader;
        this.pageBuilder  = pageBuilder;

        buildTimestampParserMap();
        buildTimestampFormatterMap();

        JsonCaster jsonCaster = new JsonCaster(task, timestampParserMap, timestampFormatterMap);
        this.jsonVisitor = new JsonVisitor(task, jsonCaster);
    }

    private void buildTimestampParserMap()
    {
        // columnName or jsonPath => TimestampParser
        for (ColumnConfig columnConfig : task.getColumns()) {
            TimestampParser parser = getTimestampParser(columnConfig, task);
            this.timestampParserMap.put(columnConfig.getName(), parser);
        }
    }

    private void buildTimestampFormatterMap()
    {
        // columnName or jsonPath => TimestampFormatter
        for (ColumnConfig columnConfig : task.getColumns()) {
            TimestampFormatter parser = getTimestampFormatter(columnConfig, task);
            this.timestampFormatterMap.put(columnConfig.getName(), parser);
        }
    }

    private TimestampParser getTimestampParser(ColumnConfig columnConfig, PluginTask task)
    {
        DateTimeZone timezone = columnConfig.getFromTimeZone().or(task.getDefaultFromTimeZone());
        List<String> formatList = columnConfig.getFromFormat().or(task.getDefaultFromTimestampFormat());
        return new TimestampParser(task.getJRuby(), formatList, timezone);
    }

    private TimestampFormatter getTimestampFormatter(ColumnConfig columnConfig, PluginTask task)
    {
        String format = columnConfig.getToFormat().or(task.getDefaultToTimestampFormat());
        DateTimeZone timezone = columnConfig.getToTimeZone().or(task.getDefaultToTimeZone());
        return new TimestampFormatter(task.getJRuby(), format, timezone);
    }

    public void setFromString(Column outputColumn, String value)
    {
        Type outputType = outputColumn.getType();
        TimestampParser timestampParser = timestampParserMap.get(outputColumn.getName());
        if (outputType instanceof StringType) {
            TimestampFormatter timestampFormatter = timestampFormatterMap.get(outputColumn.getName());
            pageBuilder.setString(outputColumn, StringCast.asString(value, timestampParser, timestampFormatter));
        }
        else if (outputType instanceof TimestampType) {
            pageBuilder.setTimestamp(outputColumn, StringCast.asTimestamp(value, timestampParser));
        }
        else if (outputType instanceof LongType) {
            pageBuilder.setLong(outputColumn, StringCast.asLong(value, timestampParser));
        }
        else if (outputType instanceof DoubleType) {
            pageBuilder.setDouble(outputColumn, StringCast.asDouble(value, timestampParser));
        }
        else {
            assert false;
        }
    }

    public void setFromTimestamp(Column outputColumn, Timestamp value)
    {
        Type outputType = outputColumn.getType();
        if (outputType instanceof StringType) {
            TimestampFormatter timestampFormatter = timestampFormatterMap.get(outputColumn.getName());
            pageBuilder.setString(outputColumn, TimestampCast.asString(value, timestampFormatter));
        }
        else if (outputType instanceof TimestampType) {
            pageBuilder.setTimestamp(outputColumn, value);
        }
        else if (outputType instanceof LongType) {
            pageBuilder.setLong(outputColumn, TimestampCast.asLong(value));
        }
        else if (outputType instanceof DoubleType) {
            pageBuilder.setDouble(outputColumn, TimestampCast.asDouble(value));
        }
        else {
            assert false;
        }
    }

    public void setFromJson(Column outputColumn, Value value)
    {
        String jsonPath = new StringBuilder("$.").append(outputColumn.getName()).toString();
        pageBuilder.setJson(outputColumn, jsonVisitor.visit(jsonPath, value));
    }
}
