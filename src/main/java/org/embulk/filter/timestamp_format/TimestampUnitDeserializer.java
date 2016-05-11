package org.embulk.filter.timestamp_format;

import java.util.Map;
import java.io.IOException;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;

public class TimestampUnitDeserializer
        extends FromStringDeserializer<TimestampUnit>
{
    private static final Map<String, TimestampUnit> stringToTimestampUnitMap;

    static {
        ImmutableMap.Builder<String, TimestampUnit> builder = ImmutableMap.builder();
        builder.put("Second", TimestampUnit.Second);
        builder.put("second", TimestampUnit.Second);
        builder.put("sec", TimestampUnit.Second);
        builder.put("MilliSecond", TimestampUnit.MilliSecond);
        builder.put("millisecond", TimestampUnit.MilliSecond);
        builder.put("milli_second", TimestampUnit.MilliSecond);
        builder.put("milli", TimestampUnit.MilliSecond);
        builder.put("msec", TimestampUnit.MilliSecond);
        builder.put("ms", TimestampUnit.MilliSecond);
        builder.put("MicroSecond", TimestampUnit.MicroSecond);
        builder.put("microsecond", TimestampUnit.MicroSecond);
        builder.put("micro_second", TimestampUnit.MicroSecond);
        builder.put("micro", TimestampUnit.MicroSecond);
        builder.put("usec", TimestampUnit.MicroSecond);
        builder.put("us", TimestampUnit.MicroSecond);
        builder.put("NanoSecond", TimestampUnit.NanoSecond);
        builder.put("nanosecond", TimestampUnit.NanoSecond);
        builder.put("nano_second", TimestampUnit.NanoSecond);
        builder.put("nano", TimestampUnit.NanoSecond);
        builder.put("nsec", TimestampUnit.NanoSecond);
        builder.put("ns", TimestampUnit.NanoSecond);
        stringToTimestampUnitMap = builder.build();
    }

    public TimestampUnitDeserializer()
    {
        super(TimestampUnit.class);
    }

    @Override
    protected TimestampUnit _deserialize(String value, DeserializationContext context)
            throws IOException
    {
        TimestampUnit t = stringToTimestampUnitMap.get(value);
        if (t == null) {
            throw new JsonMappingException(
                    String.format("Unknown type name '%s'. Supported types are: %s",
                            value,
                            Joiner.on(", ").join(stringToTimestampUnitMap.keySet())));
        }
        return t;
    }
}
