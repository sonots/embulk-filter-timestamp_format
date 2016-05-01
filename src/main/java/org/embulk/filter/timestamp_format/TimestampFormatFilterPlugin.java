package org.embulk.filter.timestamp_format;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;

import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;

import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.BooleanType;
import org.embulk.spi.type.JsonType;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;
import org.jruby.embed.ScriptingContainer;
import org.slf4j.Logger;

import java.util.List;

public class TimestampFormatFilterPlugin implements FilterPlugin
{
    private static final Logger logger = Exec.getLogger(TimestampFormatFilterPlugin.class);

    public TimestampFormatFilterPlugin() {}

    // NOTE: This is not spi.ColumnConfig
    interface ColumnConfig extends Task,
            TimestampParser.TimestampColumnOption, TimestampFormatter.TimestampColumnOption
    {
        @Config("name")
        String getName();

        @Config("type")
        @ConfigDefault("\"string\"")
        Type getType();

        @Config("from_unit")
        @ConfigDefault("null")
        Optional<TimestampUnit> getFromUnit();

        @Config("to_unit")
        @ConfigDefault("null")
        Optional<TimestampUnit> getToUnit();
    }

    interface PluginTask extends Task,
            TimestampParser.Task, TimestampFormatter.Task
    {
        @Config("columns")
        @ConfigDefault("[]")
        List<ColumnConfig> getColumns();

        @Config("stop_on_invalid_record")
        @ConfigDefault("false")
        Boolean getStopOnInvalidRecord();

        @Config("default_from_timestamp_unit")
        @ConfigDefault("\"second\"")
        TimestampUnit getDefaultFromTimestampUnit();

        @Config("default_to_timestamp_unit")
        @ConfigDefault("\"second\"")
        TimestampUnit getDefaultToTimestampUnit();

        @ConfigInject
        ScriptingContainer getJRuby();
    }

    @Override
    public void transaction(final ConfigSource config, final Schema inputSchema,
            final FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        configure(task, inputSchema);
        Schema outputSchema = buildOuputSchema(task, inputSchema);
        control.run(task.dump(), outputSchema);
    }

    private void configure(PluginTask task, Schema inputSchema)
    {
        List<ColumnConfig> columns = task.getColumns();

        // throw if column does not exist
        for (ColumnConfig columnConfig : columns) {
            String name = columnConfig.getName();
            if (name.startsWith("$.")) {
                String firstName = name.split("\\.", 3)[1]; // check only top level column name
                inputSchema.lookupColumn(firstName);
            }
            else {
                inputSchema.lookupColumn(name);
            }
        }

        // throw if column type is not supported
        for (ColumnConfig columnConfig : columns) {
            String name = columnConfig.getName();
            Type type = columnConfig.getType();
            if (type instanceof BooleanType) {
                throw new ConfigException(String.format("casting to boolean is not available: \"%s\"", name));
            }
            if (type instanceof JsonType) {
                throw new ConfigException(String.format("casting to json is not available: \"%s\"", name));
            }
            if (name.startsWith("$.") && type instanceof TimestampType) {
                throw new ConfigException(String.format("casting a json path into timestamp is not available: \"%s\"", name));
            }
        }
    }

    private Schema buildOuputSchema(final PluginTask task, final Schema inputSchema)
    {
        List<ColumnConfig> columnConfigs = task.getColumns();
        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        int i = 0;
        for (Column inputColumn : inputSchema.getColumns()) {
            String name = inputColumn.getName();
            Type   type = inputColumn.getType();
            ColumnConfig columnConfig = getColumnConfig(name, columnConfigs);
            if (columnConfig != null) {
                type = columnConfig.getType();
            }
            Column outputColumn = new Column(i++, name, type);
            builder.add(outputColumn);
        }
        return new Schema(builder.build());
    }

    private ColumnConfig getColumnConfig(String name, List<ColumnConfig> columnConfigs)
    {
        // hash should be faster, though
        for (ColumnConfig columnConfig : columnConfigs) {
            if (columnConfig.getName().equals(name)) {
                return columnConfig;
            }
        }
        return null;
    }

    @Override
    public PageOutput open(final TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);

        return new PageOutput() {
            private PageReader pageReader = new PageReader(inputSchema);
            private PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
            private ColumnVisitorImpl visitor = new ColumnVisitorImpl(task, inputSchema, outputSchema, pageReader, pageBuilder);

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
                    inputSchema.visitColumns(visitor);
                    pageBuilder.addRecord();
                }
            }
        };
    }
}
