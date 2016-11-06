package org.embulk.filter.timestamp_format;

import io.github.medjed.jsonpathcompiler.expressions.Path;
import io.github.medjed.jsonpathcompiler.expressions.path.ArrayPathToken;
import io.github.medjed.jsonpathcompiler.expressions.path.PathCompiler;
import io.github.medjed.jsonpathcompiler.expressions.path.PathToken;
import io.github.medjed.jsonpathcompiler.expressions.path.PropertyPathToken;
import org.embulk.filter.timestamp_format.TimestampFormatFilterPlugin.ColumnConfig;
import org.embulk.filter.timestamp_format.TimestampFormatFilterPlugin.PluginTask;

import org.embulk.spi.Exec;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import org.slf4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class JsonVisitor
{
    private static final Logger logger = Exec.getLogger(TimestampFormatFilterPlugin.class);
    private final PluginTask task;
    private final JsonCaster jsonCaster;
    private final HashMap<String, ColumnConfig> jsonPathColumnConfigMap = new HashMap<>();
    private final HashSet<String> shouldVisitSet = new HashSet<>();

    JsonVisitor(PluginTask task, JsonCaster jsonCaster)
    {
        this.task = task;
        this.jsonCaster = jsonCaster;

        assertJsonPathFormat();
        buildJsonPathColumnConfigMap();
        buildShouldVisitSet();
    }

    private void assertJsonPathFormat()
    {
        for (ColumnConfig columnConfig : task.getColumns()) {
            String name = columnConfig.getName();
            if (!PathCompiler.isProbablyJsonPath(name)) {
                continue;
            }
            JsonPathUtil.assertJsonPathFormat(name);
        }
    }

    private void buildJsonPathColumnConfigMap()
    {
        // json path => Type
        for (ColumnConfig columnConfig : task.getColumns()) {
            String name = columnConfig.getName();
            if (!PathCompiler.isProbablyJsonPath(name)) {
                continue;
            }
            Path compiledPath = PathCompiler.compile(name);
            this.jsonPathColumnConfigMap.put(compiledPath.toString(), columnConfig);
        }
    }

    private void buildShouldVisitSet()
    {
        // json partial path => Boolean to avoid unnecessary type: json visit
        for (ColumnConfig columnConfig : task.getColumns()) {
            String name = columnConfig.getName();
            if (! PathCompiler.isProbablyJsonPath(name)) {
                continue;
            }
            Path compiledPath = PathCompiler.compile(name);
            PathToken parts = compiledPath.getRoot();
            StringBuilder partialPath = new StringBuilder("$");
            while (! parts.isLeaf()) {
                parts = parts.next(); // first next() skips "$"
                partialPath.append(parts.getPathFragment());
                this.shouldVisitSet.add(partialPath.toString());
            }
        }
    }

    private boolean shouldVisit(String jsonPath)
    {
        return shouldVisitSet.contains(jsonPath);
    }

    public Value visit(String rootPath, Value value)
    {
        if (!shouldVisit(rootPath)) {
            return value;
        }
        if (value.isArrayValue()) {
            ArrayValue arrayValue = value.asArrayValue();
            int size = arrayValue.size();
            Value[] newValue = new Value[size];
            for (int i = 0; i < size; i++) {
                String pathFragment = ArrayPathToken.getPathFragment(i);
                String k = new StringBuilder(rootPath).append(pathFragment).toString();
                if (!shouldVisit(k)) {
                    k = new StringBuilder(rootPath).append("[*]").toString(); // try [*] too
                }
                Value v = arrayValue.get(i);
                newValue[i] = visit(k, v);
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
                String pathFragment = PropertyPathToken.getPathFragment(k.asStringValue().asString());
                String newPath = new StringBuilder(rootPath).append(pathFragment).toString();
                Value r = visit(newPath, v);
                newValue[i++] = k;
                newValue[i++] = r;
            }
            return ValueFactory.newMap(newValue, true);
        }
        else if (value.isIntegerValue()) {
            ColumnConfig columnConfig = jsonPathColumnConfigMap.get(rootPath);
            return jsonCaster.fromLong(columnConfig, value.asIntegerValue());
        }
        else if (value.isFloatValue()) {
            ColumnConfig columnConfig = jsonPathColumnConfigMap.get(rootPath);
            return jsonCaster.fromDouble(columnConfig, value.asFloatValue());
        }
        else if (value.isStringValue()) {
            ColumnConfig columnConfig = jsonPathColumnConfigMap.get(rootPath);
            return jsonCaster.fromString(columnConfig, value.asStringValue());
        }
        else {
            return value;
        }
    }
}
