package org.embulk.filter;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;

import org.embulk.config.Config;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;

import org.embulk.filter.timestamp_format.TimestampParser;
import org.embulk.filter.timestamp_format.TimestampFormatter;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParseException;

import org.jruby.embed.ScriptingContainer;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

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

        control.run(task.dump(), inputSchema);
    }

    private TimestampParser getTimestampParser(String name, PluginTask task)
    {
        for (ColumnConfig columnConfig : task.getColumns()) {
            if (columnConfig.getName().equals(name)) {
                List<TimestampParser> timestampParser = new ArrayList<TimestampParser>();
                DateTimeZone timezone = columnConfig.getFromTimeZone().or(task.getDefaultFromTimeZone());
                List<String> formatList = columnConfig.getFromFormat().or(task.getDefaultFromTimestampFormat());
                return new TimestampParser(task.getJRuby(), formatList, timezone);
            }
        }
        return null;
    }

    private TimestampFormatter getTimestampFormatter(String name, PluginTask task)
    {
        for (ColumnConfig columnConfig : task.getColumns()) {
            if (columnConfig.getName().equals(name)) {
                String format = columnConfig.getToFormat().or(task.getDefaultToTimestampFormat());
                DateTimeZone timezone = columnConfig.getToTimeZone().or(task.getDefaultToTimeZone());
                return new TimestampFormatter(task.getJRuby(), format, timezone);
            }
        }
        return null;
    }

    @Override
    public PageOutput open(final TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);

        // columnName => TimesatmpParser
        final HashMap<String, TimestampParser> timestampParserMap = new HashMap<String, TimestampParser>();
        for (Column outputColumn : outputSchema.getColumns()) {
            String name = outputColumn.getName();
            TimestampParser parser = getTimestampParser(name, task);
            timestampParserMap.put(name, parser); // NOTE: value would be null
        }
        // columnName => TimesatmpFormatter
        final HashMap<String, TimestampFormatter> timestampFormatterMap = new HashMap<String, TimestampFormatter>();
        for (Column outputColumn : outputSchema.getColumns()) {
            String name = outputColumn.getName();
            TimestampFormatter parser = getTimestampFormatter(name, task);
            timestampFormatterMap.put(name, parser); // NOTE: value would be null
        }

        return new PageOutput() {
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

                ColumnVisitorImpl(PageBuilder pageBuilder) {
                    this.pageBuilder = pageBuilder;
                }

                @Override
                public void booleanColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        pageBuilder.setNull(column);
                    } else {
                        pageBuilder.setBoolean(column, pageReader.getBoolean(column));
                    }
                }

                @Override
                public void longColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        pageBuilder.setNull(column);
                    } else {
                        pageBuilder.setLong(column, pageReader.getLong(column));
                    }
                }

                @Override
                public void doubleColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        pageBuilder.setNull(column);
                    } else {
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
                    String name = column.getName();
                    TimestampFormatter formatter = timestampFormatterMap.get(name);
                    TimestampParser parser = timestampParserMap.get(name);

                    if (formatter == null || parser == null) {
                        pageBuilder.setString(column, pageReader.getString(column));
                        return;
                    }

                    String value = pageReader.getString(column);
                    try {
                        Timestamp timestamp = parser.parse(value);
                        pageBuilder.setString(column, formatter.format(timestamp));
                    }
                    catch (TimestampParseException ex) {
                        if (task.getStopOnInvalidRecord()) {
                            throw Throwables.propagate(ex);
                        } else {
                            logger.warn("invalid value \"{}\"", value);
                            pageBuilder.setNull(column);
                        }
                    }
                }

                @Override
                public void jsonColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        pageBuilder.setNull(column);
                    }
                    else {
                        pageBuilder.setJson(column, pageReader.getJson(column));
                    }
                }

                @Override
                public void timestampColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        pageBuilder.setNull(column);
                    } else {
                        pageBuilder.setTimestamp(column, pageReader.getTimestamp(column));
                    }
                }
            }
        };
    }
}
