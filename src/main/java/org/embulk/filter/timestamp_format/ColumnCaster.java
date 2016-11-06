package org.embulk.filter.timestamp_format;

import io.github.medjed.jsonpathcompiler.expressions.path.PropertyPathToken;
import org.embulk.filter.timestamp_format.cast.DoubleCast;
import org.embulk.filter.timestamp_format.cast.LongCast;
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

import java.util.ArrayList;
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
    private final HashMap<String, TimestampUnit> fromTimestampUnitMap = new HashMap<>();
    private final HashMap<String, TimestampUnit> toTimestampUnitMap = new HashMap<>();
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
        buildFromTimestampUnitMap();
        buildToTimestampUnitMap();

        JsonCaster jsonCaster = new JsonCaster(task, timestampParserMap, timestampFormatterMap, fromTimestampUnitMap, toTimestampUnitMap);
        this.jsonVisitor = new JsonVisitor(task, jsonCaster);
    }

    private void buildTimestampParserMap()
    {
        // columnName or jsonPath => TimestampParser
        // we do not know input type of json here, so creates anyway
        for (ColumnConfig columnConfig : task.getColumns()) {
            TimestampParser parser = getTimestampParser(columnConfig, task);
            this.timestampParserMap.put(columnConfig.getName(), parser);
        }
    }

    private TimestampParser getTimestampParser(ColumnConfig columnConfig, PluginTask task)
    {
        DateTimeZone timezone = columnConfig.getFromTimeZone().or(task.getDefaultFromTimeZone());
        List<String> formatList = columnConfig.getFromFormat().or(task.getDefaultFromTimestampFormat());
        List<String> newFormatList = new ArrayList<>(formatList);
        String name = columnConfig.getName();
        if (task.getTimeStampParser().equals("auto_java")) {
            for (int i = 0; i < formatList.size(); i++) {
                String format = formatList.get(i);
                if (!format.contains("%")) {
                    continue;
                }
                String javaFormat = TimestampFormatConverter.toJavaFormat(format);
                if (javaFormat == null) {
                    logger.info(String.format("%s: Failed to convert ruby parser to java parser: \"%s\", Use ruby parser as is", name, format));
                } else {
                    logger.debug(String.format("%s: Convert ruby parser \"%s\" to java parser \"%s\"", name, format, javaFormat));
                    newFormatList.set(i, javaFormat);
                }
            }
        }
        return new TimestampParser(task.getJRuby(), newFormatList, timezone);
    }

    private void buildTimestampFormatterMap()
    {
        // columnName or jsonPath => TimestampFormatter
        for (ColumnConfig columnConfig : task.getColumns()) {
            if (columnConfig.getType() instanceof StringType) {
                TimestampFormatter parser = getTimestampFormatter(columnConfig, task);
                this.timestampFormatterMap.put(columnConfig.getName(), parser);
            }
        }
    }

    private TimestampFormatter getTimestampFormatter(ColumnConfig columnConfig, PluginTask task)
    {
        String format = columnConfig.getToFormat().or(task.getDefaultToTimestampFormat());
        DateTimeZone timezone = columnConfig.getToTimeZone().or(task.getDefaultToTimeZone());
        return new TimestampFormatter(task.getJRuby(), format, timezone);
    }

    private void buildFromTimestampUnitMap()
    {
        // columnName or jsonPath => TimestampUnit
        // we do not know input type of json here, so creates anyway
        for (ColumnConfig columnConfig : task.getColumns()) {
            TimestampUnit unit = getFromTimestampUnit(columnConfig, task);
            this.fromTimestampUnitMap.put(columnConfig.getName(), unit);
        }
    }

    private TimestampUnit getFromTimestampUnit(ColumnConfig columnConfig, PluginTask task)
    {
        return columnConfig.getFromUnit().or(task.getDefaultFromTimestampUnit());
    }

    private void buildToTimestampUnitMap()
    {
        // columnName or jsonPath => TimestampUnit
        for (ColumnConfig columnConfig : task.getColumns()) {
            Type type = columnConfig.getType();
            if (type instanceof LongType || type instanceof DoubleType) {
                TimestampUnit unit = getToTimestampUnit(columnConfig, task);
                this.toTimestampUnitMap.put(columnConfig.getName(), unit);
            }
        }
    }

    private TimestampUnit getToTimestampUnit(ColumnConfig columnConfig, PluginTask task)
    {
        return columnConfig.getToUnit().or(task.getDefaultToTimestampUnit());
    }

    public void setFromLong(Column outputColumn, long value)
    {
        Type outputType = outputColumn.getType();
        TimestampUnit fromUnit = fromTimestampUnitMap.get(outputColumn.getName());
        if (outputType instanceof StringType) {
            TimestampFormatter timestampFormatter = timestampFormatterMap.get(outputColumn.getName());
            pageBuilder.setString(outputColumn, LongCast.asString(value, fromUnit, timestampFormatter));
        }
        else if (outputType instanceof TimestampType) {
            pageBuilder.setTimestamp(outputColumn, LongCast.asTimestamp(value, fromUnit));
        }
        else if (outputType instanceof LongType) {
            TimestampUnit toUnit = toTimestampUnitMap.get(outputColumn.getName());
            pageBuilder.setLong(outputColumn, LongCast.asLong(value, fromUnit, toUnit));
        }
        else if (outputType instanceof DoubleType) {
            TimestampUnit toUnit = toTimestampUnitMap.get(outputColumn.getName());
            pageBuilder.setDouble(outputColumn, LongCast.asDouble(value, fromUnit, toUnit));
        }
        else {
            assert false;
        }
    }

    public void setFromDouble(Column outputColumn, double value)
    {
        Type outputType = outputColumn.getType();
        TimestampUnit fromUnit = fromTimestampUnitMap.get(outputColumn.getName());
        if (outputType instanceof StringType) {
            TimestampFormatter timestampFormatter = timestampFormatterMap.get(outputColumn.getName());
            pageBuilder.setString(outputColumn, DoubleCast.asString(value, fromUnit, timestampFormatter));
        }
        else if (outputType instanceof TimestampType) {
            pageBuilder.setTimestamp(outputColumn, DoubleCast.asTimestamp(value, fromUnit));
        }
        else if (outputType instanceof LongType) {
            TimestampUnit toUnit = toTimestampUnitMap.get(outputColumn.getName());
            pageBuilder.setLong(outputColumn, DoubleCast.asLong(value, fromUnit, toUnit));
        }
        else if (outputType instanceof DoubleType) {
            TimestampUnit toUnit = toTimestampUnitMap.get(outputColumn.getName());
            pageBuilder.setDouble(outputColumn, DoubleCast.asDouble(value, fromUnit, toUnit));
        }
        else {
            assert false;
        }
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
            TimestampUnit toUnit = toTimestampUnitMap.get(outputColumn.getName());
            pageBuilder.setLong(outputColumn, StringCast.asLong(value, timestampParser, toUnit));
        }
        else if (outputType instanceof DoubleType) {
            TimestampUnit toUnit = toTimestampUnitMap.get(outputColumn.getName());
            pageBuilder.setDouble(outputColumn, StringCast.asDouble(value, timestampParser, toUnit));
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
            TimestampUnit toUnit = toTimestampUnitMap.get(outputColumn.getName());
            pageBuilder.setLong(outputColumn, TimestampCast.asLong(value, toUnit));
        }
        else if (outputType instanceof DoubleType) {
            TimestampUnit toUnit = toTimestampUnitMap.get(outputColumn.getName());
            pageBuilder.setDouble(outputColumn, TimestampCast.asDouble(value, toUnit));
        }
        else {
            assert false;
        }
    }

    public void setFromJson(Column outputColumn, Value value)
    {
        String pathFragment = PropertyPathToken.getPathFragment(outputColumn.getName());
        String jsonPath = new StringBuilder("$").append(pathFragment).toString();
        pageBuilder.setJson(outputColumn, jsonVisitor.visit(jsonPath, value));
    }
}
