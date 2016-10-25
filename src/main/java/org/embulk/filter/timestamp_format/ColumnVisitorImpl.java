package org.embulk.filter.timestamp_format;

import org.embulk.spi.DataException;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;

import org.embulk.filter.timestamp_format.TimestampFormatFilterPlugin.ColumnConfig;
import org.embulk.filter.timestamp_format.TimestampFormatFilterPlugin.PluginTask;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.HashSet;

public class ColumnVisitorImpl
        implements ColumnVisitor
{
    private static final Logger logger = Exec.getLogger(TimestampFormatFilterPlugin.class);
    private final PluginTask task;
    private final Schema inputSchema;
    private final Schema outputSchema;
    private final PageReader pageReader;
    private final PageBuilder pageBuilder;
    private final HashSet<String> shouldCastSet = new HashSet<>();
    private final HashMap<String, Column> outputColumnMap = new HashMap<>();
    private final ColumnCaster columnCaster;

    ColumnVisitorImpl(PluginTask task, Schema inputSchema, Schema outputSchema,
                      PageReader pageReader, PageBuilder pageBuilder)
    {
        this.task         = task;
        this.inputSchema  = inputSchema;
        this.outputSchema = outputSchema;
        this.pageReader   = pageReader;
        this.pageBuilder  = pageBuilder;

        buildShouldCastSet();
        buildOutputColumnMap();
        this.columnCaster = new ColumnCaster(task, inputSchema, outputSchema, pageReader, pageBuilder);
    }

    private void buildShouldCastSet()
    {
        // columnName => Boolean to avoid unnecessary cast
        for (ColumnConfig columnConfig : task.getColumns()) {
            String name = columnConfig.getName();
            if (name.startsWith("$.")) {
                String firstName = name.split("\\.", 3)[1]; // check only top level column name
                String firstPartName = firstName.split("\\[")[0];
                shouldCastSet.add(firstPartName);
                continue;
            }
            shouldCastSet.add(name);
        }
    }

    private boolean shouldCast(String name)
    {
        return shouldCastSet.contains(name);
    }

    private void buildOutputColumnMap()
    {
        // columnName => outputColumn
        for (Column column : outputSchema.getColumns()) {
            this.outputColumnMap.put(column.getName(), column);
        }
    }

    private interface PageBuildable
    {
        public void run() throws DataException;
    }

    private void withStopOnInvalidRecord(final PageBuildable op,
                                         final Column inputColumn, final Column outputColumn) throws DataException
    {
        if (pageReader.isNull(inputColumn)) {
            pageBuilder.setNull(outputColumn);
        }
        else {
            if (task.getStopOnInvalidRecord()) {
                op.run();
            }
            else {
                try {
                    op.run();
                }
                catch (final DataException ex) {
                    logger.warn(ex.getMessage());
                    pageBuilder.setNull(outputColumn);
                }
            }
        }
    }

    @Override
    public void booleanColumn(final Column inputColumn)
    {
        if (pageReader.isNull(inputColumn)) {
            pageBuilder.setNull(inputColumn);
        }
        else {
            pageBuilder.setBoolean(inputColumn, pageReader.getBoolean(inputColumn));
        }
    }

    @Override
    public void longColumn(final Column inputColumn)
    {
        String name = inputColumn.getName();
        if (! shouldCast(name)){
            if (pageReader.isNull(inputColumn)) {
                pageBuilder.setNull(inputColumn);
            }
            else {
                pageBuilder.setLong(inputColumn, pageReader.getLong(inputColumn));
            }
        }
        else {
            final Column outputColumn = outputColumnMap.get(name);
            PageBuildable op = new PageBuildable() {
                public void run() throws DataException {
                    columnCaster.setFromLong(outputColumn, pageReader.getLong(inputColumn));
                }
            };
            withStopOnInvalidRecord(op, inputColumn, outputColumn);
        }
    }

    @Override
    public void doubleColumn(final Column inputColumn)
    {
        String name = inputColumn.getName();
        if (! shouldCast(name)){
            if (pageReader.isNull(inputColumn)) {
                pageBuilder.setNull(inputColumn);
            }
            else {
                pageBuilder.setDouble(inputColumn, pageReader.getDouble(inputColumn));
            }
        }
        else {
            final Column outputColumn = outputColumnMap.get(inputColumn.getName());
            PageBuildable op = new PageBuildable() {
                public void run() throws DataException {
                    columnCaster.setFromDouble(outputColumn, pageReader.getDouble(inputColumn));
                }
            };
            withStopOnInvalidRecord(op, inputColumn, outputColumn);
        }
    }

    @Override
    public void stringColumn(final Column inputColumn)
    {
        String name = inputColumn.getName();
        if (! shouldCast(name)){
            if (pageReader.isNull(inputColumn)) {
                pageBuilder.setNull(inputColumn);
            }
            else {
                pageBuilder.setString(inputColumn, pageReader.getString(inputColumn));
            }
        }
        else {
            final Column outputColumn = outputColumnMap.get(inputColumn.getName());
            PageBuildable op = new PageBuildable() {
                public void run() throws DataException {
                    columnCaster.setFromString(outputColumn, pageReader.getString(inputColumn));
                }
            };
            withStopOnInvalidRecord(op, inputColumn, outputColumn);
        }
    }

    @Override
    public void timestampColumn(final Column inputColumn)
    {
        String name = inputColumn.getName();
        if (! shouldCast(name)){
            if (pageReader.isNull(inputColumn)) {
                pageBuilder.setNull(inputColumn);
            }
            else {
                pageBuilder.setTimestamp(inputColumn, pageReader.getTimestamp(inputColumn));
            }
        }
        else {
            final Column outputColumn = outputColumnMap.get(inputColumn.getName());
            PageBuildable op = new PageBuildable() {
                public void run() throws DataException {
                    columnCaster.setFromTimestamp(outputColumn, pageReader.getTimestamp(inputColumn));
                }
            };
            withStopOnInvalidRecord(op, inputColumn, outputColumn);
        }
    }

    @Override
    public void jsonColumn(final Column inputColumn)
    {
        String name = inputColumn.getName();
        if (! shouldCast(name)){
            if (pageReader.isNull(inputColumn)) {
                pageBuilder.setNull(inputColumn);
            }
            else {
                pageBuilder.setJson(inputColumn, pageReader.getJson(inputColumn));
            }
        }
        else {
            final Column outputColumn = outputColumnMap.get(inputColumn.getName());
            PageBuildable op = new PageBuildable() {
                public void run() throws DataException {
                    columnCaster.setFromJson(outputColumn, pageReader.getJson(inputColumn));
                }
            };
            withStopOnInvalidRecord(op, inputColumn, outputColumn);
        }
    }
}
