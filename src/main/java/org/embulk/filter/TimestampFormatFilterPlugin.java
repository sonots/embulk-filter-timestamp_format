package org.embulk.filter;

import com.google.common.base.Throwables;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;

import org.embulk.filter.timestamp_format.TimestampFormatter;
import org.embulk.filter.timestamp_format.TimestampParser;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParseException;

import org.joda.time.DateTimeZone;
import org.jruby.embed.ScriptingContainer;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TimestampFormatFilterPlugin implements FilterPlugin
{
    private static final Logger logger = Exec.getLogger(TimestampFormatFilterPlugin.class);

    public TimestampFormatFilterPlugin()
    {
    }

    // NOTE: This is not spi.ColumnConfig
    private interface ColumnConfig extends Task,
            TimestampParser.TimestampColumnOption, TimestampFormatter.TimestampColumnOption
    {
        @Config("name")
        String getName();
    }

    public interface PluginTask extends Task,
           TimestampParser.Task, TimestampFormatter.Task
    {
        @Config("columns")
        @ConfigDefault("[]")
        List<ColumnConfig> getColumns();

        @Config("stop_on_invalid_record")
        @ConfigDefault("false")
        Boolean getStopOnInvalidRecord();

        @ConfigInject
        ScriptingContainer getJRuby();
    }

    @Override
    public void transaction(final ConfigSource config, final Schema inputSchema,
            final FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        List<ColumnConfig> columns = task.getColumns();

        if (columns.size() == 0) {
            throw new ConfigException("\"columns\" must be specified.");
        }

        for (ColumnConfig columnConfig : columns) {
            String name = columnConfig.getName();
            if (!name.startsWith("$.")) {
                inputSchema.lookupColumn(name); // throw Column 'name' is not found
            }
        }

        control.run(task.dump(), inputSchema);
    }

    private TimestampParser getTimestampParser(ColumnConfig columnConfig, PluginTask task)
    {
        List<TimestampParser> timestampParser = new ArrayList<TimestampParser>();
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

    @Override
    public PageOutput open(final TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);

        // columnName => TimestampParser
        final HashMap<String, TimestampParser> timestampParserMap = new HashMap<String, TimestampParser>();
        for (ColumnConfig columnConfig : task.getColumns()) {
            TimestampParser parser = getTimestampParser(columnConfig, task);
            timestampParserMap.put(columnConfig.getName(), parser); // NOTE: value would be null
        }
        // columnName => TimestampFormatter
        final HashMap<String, TimestampFormatter> timestampFormatterMap = new HashMap<String, TimestampFormatter>();
        for (ColumnConfig columnConfig : task.getColumns()) {
            TimestampFormatter parser = getTimestampFormatter(columnConfig, task);
            timestampFormatterMap.put(columnConfig.getName(), parser); // NOTE: value would be null
        }

        return new PageOutput() {
            public Value formatTimestampStringRecursively(PluginTask task, String name, Value value)
                    throws TimestampParseException
            {
                if (value.isArrayValue()) {
                    ArrayValue arrayValue = value.asArrayValue();
                    int size = arrayValue.size();
                    Value[] newValue = new Value[size];
                    for (int i = 0; i < size; i++) {
                        String k = new StringBuilder(name).append("[").append(Integer.toString(i)).append("]").toString();
                        Value v = arrayValue.get(i);
                        newValue[i] = formatTimestampStringRecursively(task, k, v);
                    }
                    return ValueFactory.newArray(newValue, true);
                }
                else if (value.isMapValue()) {
                    MapValue mapValue = value.asMapValue();
                    int size = mapValue.size() * 2;
                    Value[] newValue = new Value[size];
                    int i = 0;
                    for (Map.Entry<Value, Value> entry : mapValue.entrySet()) {
                        Value k = entry.getKey();
                        Value v = entry.getValue();
                        String newName = new StringBuilder(name).append(".").append(k.asStringValue().asString()).toString();
                        Value r = formatTimestampStringRecursively(task, newName, v);
                        newValue[i++] = k;
                        newValue[i++] = r;
                    }
                    return ValueFactory.newMap(newValue, true);
                }
                else if (value.isStringValue()) {
                    String stringValue = value.asStringValue().asString();
                    String newValue = formatTimestampString(task, name, stringValue);
                    return (Objects.equals(newValue, stringValue)) ? value : ValueFactory.newString(newValue);
                }
                else {
                    return value;
                }
            }

            public String formatTimestampString(PluginTask task, String name, String value)
                    throws TimestampParseException
            {
                TimestampParser parser = timestampParserMap.get(name);
                TimestampFormatter formatter = timestampFormatterMap.get(name);
                if (formatter == null || parser == null) {
                    return value;
                }
                try {
                    Timestamp timestamp = parser.parse(value);
                    return formatter.format(timestamp);
                }
                catch (TimestampParseException ex) {
                    if (task.getStopOnInvalidRecord()) {
                        throw Throwables.propagate(ex);
                    }
                    else {
                        logger.warn("invalid value \"{}\":\"{}\"", name, value);
                        return value;
                    }
                }
            }

            private PageReader pageReader = new PageReader(inputSchema);
            private PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
            private ColumnVisitorImpl visitor = new ColumnVisitorImpl(pageBuilder);

            @Override
            public void finish()
            {
                pageBuilder.finish();
            }

            @Override
            public void close()
            {
                pageBuilder.close();
            }

            @Override
            public void add(Page page)
            {
                pageReader.setPage(page);

                while (pageReader.nextRecord()) {
                    outputSchema.visitColumns(visitor);
                    pageBuilder.addRecord();
                }
            }

            class ColumnVisitorImpl implements ColumnVisitor
            {
                private final PageBuilder pageBuilder;

                ColumnVisitorImpl(PageBuilder pageBuilder)
                {
                    this.pageBuilder = pageBuilder;
                }

                @Override
                public void booleanColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        pageBuilder.setNull(column);
                    }
                    else {
                        pageBuilder.setBoolean(column, pageReader.getBoolean(column));
                    }
                }

                @Override
                public void longColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        pageBuilder.setNull(column);
                    }
                    else {
                        pageBuilder.setLong(column, pageReader.getLong(column));
                    }
                }

                @Override
                public void doubleColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        pageBuilder.setNull(column);
                    }
                    else {
                        pageBuilder.setDouble(column, pageReader.getDouble(column));
                    }
                }

                @Override
                public void stringColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        pageBuilder.setNull(column);
                        return;
                    }
                    String value = pageReader.getString(column);
                    String formatted = formatTimestampString(task, column.getName(), value);
                    pageBuilder.setString(column, formatted);
                }

                @Override
                public void jsonColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        pageBuilder.setNull(column);
                    }
                    else {
                        String name = new StringBuilder("$.").append(column.getName()).toString();
                        Value value = pageReader.getJson(column);
                        Value formatted = formatTimestampStringRecursively(task, name, value);
                        pageBuilder.setJson(column, formatted);
                    }
                }

                @Override
                public void timestampColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        pageBuilder.setNull(column);
                    }
                    else {
                        pageBuilder.setTimestamp(column, pageReader.getTimestamp(column));
                    }
                }
            }
        };
    }
}
