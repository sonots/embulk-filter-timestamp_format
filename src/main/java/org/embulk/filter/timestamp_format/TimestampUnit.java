package org.embulk.filter.timestamp_format;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.embulk.spi.time.Timestamp;

@JsonDeserialize(using=TimestampUnitDeserializer.class)
public enum TimestampUnit {
    Second {
        @Override
        public int scale() {
            return 1;
        }

        @Override
        public int scaleToNano() {
            return 1000000000;
        }
    },
    MilliSecond {
        @Override
        public int scale() {
            return 1000;
        }

        @Override
        public int scaleToNano() {
            return 1000000;
        }
    },
    MicroSecond {
        @Override
        public int scale() {
            return 1000000;
        }

        @Override
        public int scaleToNano() {
            return 1000;
        }
    },
    NanoSecond {
        @Override
        public int scale() {
            return 1000000000;
        }

        @Override
        public int scaleToNano() {
            return 1;
        }
    };

    public abstract int scale();
    public abstract int scaleToNano();

    public static Timestamp asTimestamp(long value, TimestampUnit fromUnit)
    {
        long nanoAdjustment = value * fromUnit.scaleToNano();
        return Timestamp.ofEpochSecond(0, nanoAdjustment);
    }

    public static Timestamp asTimestamp(double value, TimestampUnit fromUnit)
    {
        long nanoAdjustment = (long) (value * fromUnit.scaleToNano());
        return Timestamp.ofEpochSecond(0, nanoAdjustment);
    }

    public static long asLong(Timestamp value, TimestampUnit toUnit)
    {
        long epochSecond = value.getEpochSecond() * toUnit.scale();
        long nanoIntegerPart = value.getNano() / toUnit.scaleToNano();
        return epochSecond + nanoIntegerPart;

    }
    public static double asDouble(Timestamp value, TimestampUnit toUnit)
    {
        long epochSecond = value.getEpochSecond() * toUnit.scale();
        long nanoIntegerPart = value.getNano() / toUnit.scaleToNano();
        long nanoDecimalPart = value.getNano() - nanoIntegerPart;
        return epochSecond + nanoIntegerPart + (nanoDecimalPart / (double) toUnit.scale());
    }

    public static long changeUnit(long value, TimestampUnit fromUnit, TimestampUnit toUnit)
    {
        if (fromUnit.scale() == toUnit.scale()) {
            return value;
        }
        else if (fromUnit.scale() < toUnit.scale()) {
            long factor = toUnit.scale() / fromUnit.scale();
            return value * factor;
        }
        else {
            long divideFactor = fromUnit.scale() / toUnit.scale();
            return value / divideFactor;
        }
    }

    public static double changeUnit(double value, TimestampUnit fromUnit, TimestampUnit toUnit)
    {
        if (fromUnit.scale() == toUnit.scale()) {
            return value;
        }
        else if (fromUnit.scale() < toUnit.scale()) {
            long factor = toUnit.scale() / fromUnit.scale();
            return value * factor;
        }
        else {
            long divideFactor = fromUnit.scale() / toUnit.scale();
            return value / (double)divideFactor;
        }
    }
}
