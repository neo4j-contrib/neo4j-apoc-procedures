package apoc.load;

import apoc.load.util.LoadCsvConfig;
import apoc.meta.Meta;
import apoc.util.Util;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static apoc.load.util.LoadCsvConfig.DEFAULT_ARRAY_SEP;
import static apoc.util.Util.parseCharFromConfig;
import static java.util.Collections.emptyList;

public class Mapping {
    public static final Mapping EMPTY = new Mapping("", Collections.emptyMap(), LoadCsvConfig.DEFAULT_ARRAY_SEP, false);
    final String name;
    final Collection<String> nullValues;
    final Meta.Types type;
    final boolean array;
    final boolean ignore;
    final char arraySep;
    private final Pattern arrayPattern;

    public Mapping(String name, Map<String, Object> mapping, char arraySep, boolean ignore) {
        this.name = mapping.getOrDefault("name", name).toString();
        this.array = (Boolean) mapping.getOrDefault("array", false);
        this.ignore = (Boolean) mapping.getOrDefault("ignore", ignore);
        this.nullValues = (Collection<String>) mapping.getOrDefault("nullValues", emptyList());
        this.arraySep = parseCharFromConfig(mapping, "arraySep", arraySep);
        this.type = Meta.Types.from(mapping.getOrDefault("type", "STRING").toString());
        this.arrayPattern = Pattern.compile(String.valueOf(this.arraySep), Pattern.LITERAL);

        if (this.type == null) {
            // Call this out to the user explicitly because deep inside of LoadCSV and others you will get
            // NPEs that are hard to spot if this is allowed to go through.
            throw new RuntimeException("In specified mapping, there is no type by the name " +
                    mapping.getOrDefault("type", "STRING").toString());
        }
    }

    public Object convert(String value) {
        return array ? convertArray(value) : convertType(value);
    }

    private Object convertArray(String value) {
        String[] values = arrayPattern.split(value);
        List<Object> result = new ArrayList<>(values.length);
        for (String v : values) {
            result.add(convertType(v));
        }
        return result;
    }

    private Object convertType(String value) {
        if (nullValues.contains(value)) return null;
        if (type == Meta.Types.STRING) return value;
        switch (type) {
            case INTEGER: return Util.toLong(value);
            case FLOAT: return Util.toDouble(value);
            case BOOLEAN: return Util.toBoolean(value);
            case NULL: return null;
            case LIST: return Arrays.stream(arrayPattern.split(value)).map(this::convertType).collect(Collectors.toList());
            default: return value;
        }
    }
}
