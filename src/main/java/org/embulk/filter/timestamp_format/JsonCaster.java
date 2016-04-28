package org.embulk.filter.timestamp_format;

import org.embulk.filter.timestamp_format.cast.StringCast;
import org.embulk.filter.timestamp_format.TimestampFormatFilterPlugin.ColumnConfig;
import org.embulk.filter.timestamp_format.TimestampFormatFilterPlugin.PluginTask;
import org.embulk.spi.Exec;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.Type;
import org.msgpack.value.StringValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import org.slf4j.Logger;

import java.util.HashMap;

class JsonCaster
{
    private static final Logger logger = Exec.getLogger(TimestampFormatFilterPlugin.class);
    private final PluginTask task;
    private final HashMap<String, TimestampParser> timestampParserMap;
    private final HashMap<String, TimestampFormatter> timestampFormatterMap;

    JsonCaster(PluginTask task,
               HashMap<String, TimestampParser> timestampParserMap,
               HashMap<String, TimestampFormatter> timestampFormatterMap)
    {
        this.task = task;
        this.timestampParserMap = timestampParserMap;
        this.timestampFormatterMap = timestampFormatterMap;
    }

    public Value fromString(ColumnConfig columnConfig, StringValue value)
    {
        Type outputType = columnConfig.getType();
        TimestampParser parser = timestampParserMap.get(columnConfig.getName());
        if (outputType instanceof StringType) {
            TimestampFormatter formatter = timestampFormatterMap.get(columnConfig.getName());
            return ValueFactory.newString(StringCast.asString(value.asString(), parser, formatter));
        }
        else if (outputType instanceof LongType) {
            return ValueFactory.newInteger(StringCast.asLong(value.asString(), parser));
        }
        else if (outputType instanceof DoubleType) {
            return ValueFactory.newFloat(StringCast.asDouble(value.asString(), parser));
        }
        else {
            assert false;
            return null;
        }
    }
}
