package org.embulk.filter.timestamp_format;

// Convert JRuby Time Format into Java (Joda-Time) Format
// Aimed only for parser (JRuby format is too rich than Java Format in terms of formatter)

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TimestampFormatConverter
{
    public static final HashMap<String, String> RUBY_TO_JAVA_FORMAT_TABLE = new HashMap<>();
    private static final Pattern IDENTIFIER_PATTERN;
    private static final Pattern NON_IDENTIFIER_PATTERN;

    static
    {
        // %A<Friday> EEEE<Friday>
        // %B<May> MMMM<May>
        // %C<20> CC<20>
        // %D<05/13/16> MM/dd/yy<05/13/16>
        // %F<2016-05-13> yyyy-MM-dd<2016-05-13>
        // %H<09> HH<09>
        // %I<09> hh<09>
        // %L<123> SSS<123>
        // %M<02> mm<02>
        // %N<123456789> nnnnnnnnn<123456789>
        // %P<am> a<AM>
        // %R<09:02> HH:mm<09:02>
        // %S<39> ss<39>
        // %T<09:02:39> HH:mm:ss<09:02:39>
        // %U<19> w<19>
        // %V<19> w<19>
        // %W<19> w<19>
        // %X<09:02:39> HH:mm:ss<09:02:39>
        // %Y<2016> yyyy<2016>
        // %Z<UTC> z<UTC>
        // %a<Fri> EEE<Fri>
        // %b<May> MMM<May>
        // %c<Fri May 13 09:02:39 2016> EEE MMM dd HH:mm:ss yyyy<Fri May 13 09:02:39 2016>
        // %d<13> dd<13>
        // %e<13> dd<13>
        // %h<May> MMM<May>
        // %j<134> DDD<134>
        // %k< 9> HH<09>
        // %m<05> MM<05>
        // %p<AM> a<AM>
        // %r<09:02:39 AM> hh:mm:ss a<09:02:39 AM>
        // %u<5> e<5>
        // %v<13-MAY-2016> dd-MMM-yyyy<13-May-2016>
        // %w<5> e<5>
        // %x<05/13/16> MM/dd/yy<05/13/16>
        // %y<16> yy<16>
        // %z<+0000> Z<+0000>
        // %:z<+00:00> Z<+0000>
        // %::z<+00:00:00> Z<+0000>
        RUBY_TO_JAVA_FORMAT_TABLE.put("A", "EEEE");
        RUBY_TO_JAVA_FORMAT_TABLE.put("a", "EEE");
        RUBY_TO_JAVA_FORMAT_TABLE.put("B", "MMMM");
        RUBY_TO_JAVA_FORMAT_TABLE.put("b", "MMM");
        RUBY_TO_JAVA_FORMAT_TABLE.put("C", "CC");
        RUBY_TO_JAVA_FORMAT_TABLE.put("c", "EEE MMM dd HH:mm:ss yyyy");
        RUBY_TO_JAVA_FORMAT_TABLE.put("D", "MM/dd/yy");
        RUBY_TO_JAVA_FORMAT_TABLE.put("d", "dd");
        RUBY_TO_JAVA_FORMAT_TABLE.put("e", "dd");
        RUBY_TO_JAVA_FORMAT_TABLE.put("F", "yyyy-MM-dd");
        RUBY_TO_JAVA_FORMAT_TABLE.put("H", "HH");
        RUBY_TO_JAVA_FORMAT_TABLE.put("h", "MMM");
        RUBY_TO_JAVA_FORMAT_TABLE.put("I", "hh");
        RUBY_TO_JAVA_FORMAT_TABLE.put("j", "DDD");
        //RUBY_TO_JAVA_FORMAT_TABLE.put("k", "HH"); // " 9" fails with HH
        RUBY_TO_JAVA_FORMAT_TABLE.put("L", "SSS");
        //RUBY_TO_JAVA_FORMAT_TABLE.put("l", "hh"); // " 9" fails with hh
        RUBY_TO_JAVA_FORMAT_TABLE.put("M", "mm");
        RUBY_TO_JAVA_FORMAT_TABLE.put("m", "MM");
        //RUBY_TO_JAVA_FORMAT_TABLE.put("n", "");
        RUBY_TO_JAVA_FORMAT_TABLE.put("N", "nnnnnnnnn");
        RUBY_TO_JAVA_FORMAT_TABLE.put("P", "a");
        RUBY_TO_JAVA_FORMAT_TABLE.put("p", "a");
        RUBY_TO_JAVA_FORMAT_TABLE.put("R", "HH:mm");
        RUBY_TO_JAVA_FORMAT_TABLE.put("r", "hh:mm:ss a");
        RUBY_TO_JAVA_FORMAT_TABLE.put("S", "ss");
        //RUBY_TO_JAVA_FORMAT_TABLE.put("s", "")); // N/A
        RUBY_TO_JAVA_FORMAT_TABLE.put("T", "HH:mm:ss");
        //RUBY_TO_JAVA_FORMAT_TABLE.put("t", "");
        RUBY_TO_JAVA_FORMAT_TABLE.put("U", "w");
        RUBY_TO_JAVA_FORMAT_TABLE.put("u", "e");
        RUBY_TO_JAVA_FORMAT_TABLE.put("v", "dd-MMM-yyyy");
        RUBY_TO_JAVA_FORMAT_TABLE.put("V", "w");
        RUBY_TO_JAVA_FORMAT_TABLE.put("W", "w");
        RUBY_TO_JAVA_FORMAT_TABLE.put("w", "e");
        RUBY_TO_JAVA_FORMAT_TABLE.put("X", "HH:mm:ss");
        RUBY_TO_JAVA_FORMAT_TABLE.put("x", "MM/dd/yy");
        RUBY_TO_JAVA_FORMAT_TABLE.put("Y", "yyyy");
        RUBY_TO_JAVA_FORMAT_TABLE.put("y", "yy");
        RUBY_TO_JAVA_FORMAT_TABLE.put("Z", "z");
        RUBY_TO_JAVA_FORMAT_TABLE.put("z", "Z");
        RUBY_TO_JAVA_FORMAT_TABLE.put("%", "%");

        String[] array = RUBY_TO_JAVA_FORMAT_TABLE.keySet().toArray(new String[0]);
        StringBuilder keyPatternBuilder = new StringBuilder(array[0]);
        for (int i = 1; i < array.length; i++) {
            keyPatternBuilder.append(array[i]);
        }
        IDENTIFIER_PATTERN = Pattern.compile(new StringBuilder()
                .append("%[-_^#0-9:]*([")
                .append(keyPatternBuilder.toString())
                .append("])")
                .toString());

        NON_IDENTIFIER_PATTERN = Pattern.compile("(^|\\s)([^%\\s]\\S*)");
    }

    // @return returns null if appropriate java format is not available
    public static String toJavaFormat(String rubyFormat)
    {
        String quotedFormat = quoteFormat(rubyFormat);
        Matcher match = IDENTIFIER_PATTERN.matcher(quotedFormat);
        StringBuffer buf = new StringBuffer();
        while (match.find()) {
            String key = match.group(1);
            String replacement = RUBY_TO_JAVA_FORMAT_TABLE.get(key);
            match.appendReplacement(buf, replacement);
        }
        match.appendTail(buf);
        String javaFormat = buf.toString();

        if (javaFormat.contains("%")) {
            return null; // give up to use java format
        }
        else {
            return javaFormat;
        }
    }

    private static String quoteFormat(String rubyFormat)
    {
        Matcher match = NON_IDENTIFIER_PATTERN.matcher(rubyFormat);
        StringBuffer buf = new StringBuffer();
        while (match.find()) {
            String replacement = new StringBuilder().append(match.group(1)).append("'").append(match.group(2)).append("'").toString();
            match.appendReplacement(buf, replacement);
        }
        match.appendTail(buf);
        return buf.toString();
    }
}
