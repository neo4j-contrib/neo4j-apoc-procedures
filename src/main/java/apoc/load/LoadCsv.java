package apoc.load;

import apoc.export.util.CountingReader;
import apoc.util.FileUtils;
import apoc.meta.Meta;
import apoc.util.Util;
import au.com.bytecode.opencsv.CSVReader;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.util.Util.cleanUrl;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class LoadCsv {

    public static final char DEFAULT_ARRAY_SEP = ';';
    public static final char DEFAULT_SEP = ',';
    @Context
    public GraphDatabaseService db;

    enum Results {
        map, list, strings, stringMap
    }

    @Procedure
    @Description("apoc.load.csv('url',{config}) YIELD lineNo, list, map - load CSV fom URL as stream of values,\n config contains any of: {skip:1,limit:5,header:false,sep:'TAB',ignore:['tmp'],arraySep:';',mapping:{years:{type:'int',arraySep:'-',array:false,name:'age',ignore:false}}")
    public Stream<CSVResult> csv(@Name("url") String url, @Name("config") Map<String, Object> config) {
        boolean failOnError = booleanValue(config, "failOnError", true);
        try {
            CountingReader reader = FileUtils.readerFor(url);

            char separator = separator(config, "sep", DEFAULT_SEP);
            char arraySep = separator(config, "arraySep", DEFAULT_ARRAY_SEP);
            long skip = longValue(config, "skip", 0L);
            boolean hasHeader = booleanValue(config, "header", true);
            long limit = longValue(config, "limit", Long.MAX_VALUE);

            EnumSet<Results> results = EnumSet.noneOf(Results.class);
            for (String result : value(config, "results", asList("map","list"))) {
                results.add(Results.valueOf(result));
            }

            List<String> ignore = value(config, "ignore", emptyList());
            List<String> nullValues = value(config, "nullValues", emptyList());
            Map<String, Map<String, Object>> mapping = value(config, "mapping", Collections.emptyMap());
            Map<String, Mapping> mappings = createMapping(mapping, arraySep, ignore);

            CSVReader csv = new CSVReader(reader, separator);
            String[] header = getHeader(hasHeader, csv, ignore, mappings);
            boolean checkIgnore = !ignore.isEmpty() || mappings.values().stream().anyMatch( m -> m.ignore);
            return StreamSupport.stream(new CSVSpliterator(csv, header, url, skip, limit, checkIgnore,mappings, nullValues,results), false);
        } catch (IOException e) {

            if(!failOnError)
                return Stream.of(new  CSVResult(new String[0], new String[0], 0, true, Collections.emptyMap(), emptyList(), EnumSet.noneOf(Results.class)));
            else
                throw new RuntimeException("Can't read CSV from URL " + cleanUrl(url), e);
        }
    }

    private Map<String, Mapping> createMapping(Map<String, Map<String, Object>> mapping, char arraySep, List<String> ignore) {
        if (mapping.isEmpty()) return Collections.emptyMap();
        HashMap<String, Mapping> result = new HashMap<>(mapping.size());
        for (Map.Entry<String, Map<String, Object>> entry : mapping.entrySet()) {
            String name = entry.getKey();
            result.put(name, new Mapping(name, entry.getValue(), arraySep, ignore.contains(name)));
        }
        return result;
    }

    static class Mapping {
        public static final Mapping EMPTY = new Mapping("", Collections.emptyMap(), DEFAULT_ARRAY_SEP, false);
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
            this.arraySep = separator(mapping.getOrDefault("arraySep", arraySep).toString(),DEFAULT_ARRAY_SEP);
            this.type = Meta.Types.from(mapping.getOrDefault("type", "STRING").toString());
            this.arrayPattern = Pattern.compile(String.valueOf(this.arraySep), Pattern.LITERAL);

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

    private String[] getHeader(boolean hasHeader, CSVReader csv, List<String> ignore, Map<String, Mapping> mapping) throws IOException {
        if (!hasHeader) return null;
        String[] header = csv.readNext();
        if (ignore.isEmpty()) return header;

        for (int i = 0; i < header.length; i++) {
            if (ignore.contains(header[i]) || mapping.getOrDefault(header[i], Mapping.EMPTY).ignore) {
                header[i] = null;
            }
        }
        return header;
    }

    private boolean booleanValue(Map<String, Object> config, String key, boolean defaultValue) {
        if (config == null || !config.containsKey(key)) return defaultValue;
        Object value = config.get(key);
        if (value instanceof Boolean) return ((Boolean) value);
        return Boolean.parseBoolean(value.toString());
    }

    private long longValue(Map<String, Object> config, String key, long defaultValue) {
        if (config == null || !config.containsKey(key)) return defaultValue;
        Object value = config.get(key);
        if (value instanceof Number) return ((Number) value).longValue();
        return Long.parseLong(value.toString());
    }

    private <T> T value(Map<String, Object> config, String key, T defaultValue) {
        if (config == null || !config.containsKey(key)) return defaultValue;
        return (T) config.get(key);
    }

    private char separator(Map<String, Object> config, String key, char defaultValue) {
        if (config == null) return defaultValue;
        Object value = config.get(key);
        if (value == null) return defaultValue;
        return separator(value.toString(), defaultValue);
    }

    private static char separator(String separator, char defaultSep) {
        if (separator==null) return defaultSep;
        if ("TAB".equalsIgnoreCase(separator)) return '\t';
        return separator.charAt(0);
    }

    public static class CSVResult {
        public long lineNo;
        public List<Object> list;
        public List<String> strings;
        public Map<String, Object> map;
        public Map<String, String> stringMap;

        public CSVResult(String[] header, String[] list, long lineNo, boolean ignore, Map<String, Mapping> mapping, List<String> nullValues, EnumSet<Results> results) {
            this.lineNo = lineNo;
            removeNullValues(list, nullValues);

            this.strings = results.contains(Results.strings) ?
                    (List)createList(header, list, ignore, mapping, false) : emptyList();
            this.stringMap = results.contains(Results.stringMap) ?
                    (Map)createMap(header, list, ignore, mapping,false) : emptyMap();
            this.map = results.contains(Results.map) ?
                    createMap(header, list, ignore, mapping,true) : emptyMap();
            this.list = results.contains(Results.list) ?
                        createList(header, list, ignore, mapping, true) : emptyList();
        }

        public void removeNullValues(String[] list, List<String> nullValues) {
            if (nullValues.isEmpty()) return;
            for (int i = 0; i < list.length; i++) {
                if (nullValues.contains(list[i]))list[i] = null;
            }
        }

        private List<Object> createList(String[] header, String[] list, boolean ignore, Map<String, Mapping> mappings, boolean convert) {
            if (!ignore && mappings.isEmpty()) return asList((Object[]) list);
            ArrayList<Object> result = new ArrayList<>(list.length);
            for (int i = 0; i < header.length; i++) {
                String name = header[i];
                if (name == null) continue;
                Mapping mapping = mappings.get(name);
                if (mapping != null) {
                    if (mapping.ignore) continue;
                    result.add(convert ? mapping.convert(list[i]) : list[i]);
                } else {
                    result.add(list[i]);
                }
            }
            return result;
        }

        private Map<String, Object> createMap(String[] header, String[] list, boolean ignore, Map<String, Mapping> mappings, boolean convert) {
            if (header == null) return null;
            Map<String, Object> map = new LinkedHashMap<>(header.length, 1f);
            for (int i = 0; i < header.length; i++) {
                String name = header[i];
                if (ignore && name == null) continue;
                Mapping mapping = mappings.get(name);
                if (mapping == null) {
                    map.put(name, list[i]);
                } else {
                    if (mapping.ignore) continue;
                    map.put(mapping.name, convert ? mapping.convert(list[i]) : list[i]);
                }
            }
            return map;
        }
    }

    private static class CSVSpliterator extends Spliterators.AbstractSpliterator<CSVResult> {
        private final CSVReader csv;
        private final String[] header;
        private final String url;
        private final long limit;
        private final boolean ignore;
        private final Map<String, Mapping> mapping;
        private final List<String> nullValues;
        private final EnumSet<Results> results;
        long lineNo;

        public CSVSpliterator(CSVReader csv, String[] header, String url, long skip, long limit, boolean ignore, Map<String, Mapping> mapping, List<String> nullValues, EnumSet<Results> results) throws IOException {
            super(Long.MAX_VALUE, Spliterator.ORDERED);
            this.csv = csv;
            this.header = header;
            this.url = url;
            this.ignore = ignore;
            this.mapping = mapping;
            this.nullValues = nullValues;
            this.results = results;
            this.limit = skip + limit;
            lineNo = skip;
            while (skip-- > 0) {
                csv.readNext();
            }
        }

        @Override
        public boolean tryAdvance(Consumer<? super CSVResult> action) {
            try {
                String[] row = csv.readNext();
                if (row != null && lineNo < limit) {
                    action.accept(new CSVResult(header, row, lineNo++, ignore,mapping, nullValues,results));
                    return true;
                }
                return false;
            } catch (IOException e) {
                throw new RuntimeException("Error reading CSV from URL " + cleanUrl(url) + " at " + lineNo, e);
            }
        }
    }
}
