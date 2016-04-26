package org.embulk.filter.timestamp_format;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;

import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;

import org.jruby.embed.ScriptingContainer;
import org.slf4j.Logger;

import java.util.List;

public class TimestampFormatFilterPlugin implements FilterPlugin
{
    private static final Logger logger = Exec.getLogger(TimestampFormatFilterPlugin.class);

    public TimestampFormatFilterPlugin()
    {
    }

    // NOTE: This is not spi.ColumnConfig
    public interface ColumnConfig extends Task,
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
        // throw if column does not exist
        for (ColumnConfig columnConfig : columns) {
            String name = columnConfig.getName();
            if (name.startsWith("$.")) {
                String firstName = name.split("\\.", 3)[1];
                inputSchema.lookupColumn(firstName);
            }
            else {
                inputSchema.lookupColumn(name);
            }
        }

        control.run(task.dump(), inputSchema);
    }

    @Override
    public PageOutput open(final TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);

        return new PageOutput() {
            private PageReader pageReader = new PageReader(inputSchema);
            private PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
            private ColumnVisitorImpl visitor = new ColumnVisitorImpl(task, pageReader, pageBuilder);

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
        };
    }
}
