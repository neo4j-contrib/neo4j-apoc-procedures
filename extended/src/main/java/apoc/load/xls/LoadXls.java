package apoc.load.xls;

import apoc.Extended;
import apoc.export.util.CountingInputStream;
import apoc.meta.Types;
import apoc.util.FileUtils;
import apoc.util.MissingDependencyException;
import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.time.*;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.export.xls.ExportXlsHandler.XLS_MISSING_DEPS_ERROR;
import static apoc.load.xls.LoadXlsHandler.getXlsSpliterator;
import static apoc.util.DateParseUtil.dateParse;
import static apoc.util.ExtendedUtil.dateFormat;
import static apoc.util.ExtendedUtil.durationParse;
import static apoc.util.Util.*;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

@Extended
public class LoadXls {

    public static final char DEFAULT_ARRAY_SEP = ';';
    @Context
    public GraphDatabaseService db;

    @Context
    public URLAccessChecker urlAccessChecker;

    enum Results {
        map, list, strings, stringMap
    }

    // Sheet1!$A$1:$F$1
    static class Selection {
        private static final Pattern PATTERN = Pattern.compile("([a-z]+)(\\d+)?(?::([a-z]+)(\\d+)?)?", Pattern.CASE_INSENSITIVE);
        private static final int DEFAULT = -1;
        String sheet;
        int top = DEFAULT;
        int left = DEFAULT;
        int bottom = DEFAULT;
        int right = DEFAULT;

        public Selection(String selector) {
            String[] parts = selector.split("!");
            sheet = parts[0];
            if (parts.length > 1 && !parts[1].trim().isEmpty()) {
                String range = parts[1].trim().replace("$","");
                Matcher matcher = PATTERN.matcher(range);
                if (matcher.matches()) {
                    left = toCol(matcher.group(1), DEFAULT);
                    top = toRow(matcher.group(2), DEFAULT);
                    right = toCol(matcher.group(3),left);
                    if (right != DEFAULT) right++;
                    bottom = toRow(matcher.group(4), DEFAULT);
                }
            }
        }

        int getOrDefault(int value, int given) { return value == DEFAULT ? given : value; }

        private int toCol(String col, int defaultValue) {
            if (col == null || col.trim().isEmpty()) return defaultValue;
            AtomicInteger index = new AtomicInteger(0);
            return Stream.of(col.trim().toUpperCase().split(""))
                    .map(str -> new AbstractMap.SimpleEntry<>(index.getAndIncrement(), str.charAt(0) - 65))
                    .map(e -> (26 * e.getKey()) + e.getValue())
                    .reduce(0, Math::addExact);
        }
        private int toRow(String row, int defaultValue) {
            if (row == null || row.trim().isEmpty()) return defaultValue;
            row = row.trim();
            try {
                return Integer.parseInt(row) - 1;
            } catch(NumberFormatException nfe) {
                return (short)defaultValue;
            }
        }

        public void updateVertical(int firstRowNum, int lastRowNum) {
            if (top == DEFAULT) top = firstRowNum;
            if (bottom == DEFAULT) bottom = lastRowNum;
        }

        public void updateHorizontal(short firstCellNum, short lastCellNum) {
            if (left == DEFAULT) left = firstCellNum;
            if (right == DEFAULT) right = lastCellNum;
        }
    }
    @Procedure("apoc.load.xls")
    @Description("apoc.load.xls('url','selector',{config}) YIELD lineNo, list, map - load XLS fom URL as stream of row values,\n config contains any of: {skip:1,limit:5,header:false,ignore:['tmp'],arraySep:';',mapping:{years:{type:'int',arraySep:'-',array:false,name:'age',ignore:false, dateFormat:'iso_date', dateParse:['dd-MM-yyyy']}}")
    public Stream<XLSResult> xls(@Name("url") String url, @Name("selector") String selector, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        boolean failOnError = booleanValue(config, "failOnError", true);
        try (CountingInputStream stream = FileUtils.inputStreamFor(url, null, null, null, urlAccessChecker)) {
            Selection selection = new Selection(selector);

            char arraySep = separator(config, "arraySep", DEFAULT_ARRAY_SEP);
            long skip = longValue(config, "skip", 0L);
            boolean hasHeader = booleanValue(config, "header", true);
            boolean skipNulls = booleanValue(config, "skipNulls", false);
            long limit = longValue(config, "limit", Long.MAX_VALUE);

            List<String> ignore = value(config, "ignore", emptyList());
            List<Object> nullValues = value(config, "nullValues", emptyList());
            Map<String, Map<String, Object>> mapping = value(config, "mapping", Collections.emptyMap());
            Map<String, Mapping> mappings = createMapping(mapping, arraySep, ignore);

            LoadXlsHandler.XLSSpliterator xlsSpliterator = getXlsSpliterator(url, stream, selection, skip, hasHeader, limit, ignore, nullValues, mappings, skipNulls);
            return StreamSupport.stream(xlsSpliterator, false);
        } catch (NoClassDefFoundError e) {
            throw new MissingDependencyException(XLS_MISSING_DEPS_ERROR);
        } catch (Exception e) {
            if(!failOnError)
                return Stream.of(new  XLSResult(new String[0], new Object[0], 0, true, Collections.emptyMap(), emptyList()));
            else
                throw new RuntimeException("Can't read XLS from URL " + cleanUrl(url), e);
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
        final Collection<Object> nullValues;
        final Types type;
        final boolean array;
        final boolean ignore;
        final char arraySep;
        final String dateFormat;
        private final String[] dateParse;
        private final Pattern arrayPattern;

        public Mapping(String name, Map<String, Object> mapping, char arraySep, boolean ignore) {
            this.name = mapping.getOrDefault("name", name).toString();
            this.array = (Boolean) mapping.getOrDefault("array", false);
            this.ignore = (Boolean) mapping.getOrDefault("ignore", ignore);
            this.nullValues = (Collection<Object>) mapping.getOrDefault("nullValues", emptyList());
            this.arraySep = separator(mapping.getOrDefault("arraySep", arraySep).toString(),DEFAULT_ARRAY_SEP);
            this.type = Types.from(mapping.getOrDefault("type", "STRING").toString());
            this.arrayPattern = Pattern.compile(String.valueOf(this.arraySep), Pattern.LITERAL);
            this.dateFormat = mapping.getOrDefault("dateFormat", StringUtils.EMPTY).toString();
            this.dateParse = convertFormat(mapping.getOrDefault("dateParse", null));
        }

        public Object convert(Object value) {
            return array ? convertArray(value) : convertType(value);
        }

        private Object convertArray(Object value) {
            if (value == null) return emptyList();
            String[] values = arrayPattern.split(value.toString());
            List<Object> result = new ArrayList<>(values.length);
            for (String v : values) {
                result.add(convertType(v));
            }
            return result;
        }

        private Object convertType(Object value) {
            if (nullValues.contains(value) || value == null) return null;
            switch (type) { // Destination Type
                case STRING:
                    if (value instanceof TemporalAccessor && !dateFormat.isEmpty()) {
                        return dateFormat((TemporalAccessor) value, dateFormat);
                    } else {
                        return value.toString();
                    }
                case INTEGER:
                    return Util.toLong(value);
                case FLOAT:
                    return Util.toDouble(value);
                case BOOLEAN:
                    return Util.toBoolean(value);
                case NULL:
                    return null;
                case LIST:
                    return Arrays.stream(arrayPattern.split(value.toString())).map(this::convertType).collect(Collectors.toList());
                case DATE:
                    return dateParse(value.toString(), LocalDate.class, dateParse);
                case DATE_TIME:
                    return dateParse(value.toString(), ZonedDateTime.class, dateParse);
                case LOCAL_DATE_TIME:
                    return dateParse(value.toString(), LocalDateTime.class, dateParse);
                case LOCAL_TIME:
                    return dateParse(value.toString(), LocalTime.class, dateParse);
                case TIME:
                    return dateParse(value.toString(), OffsetTime.class, dateParse);
                case DURATION:
                    return durationParse(value.toString());
                default:
                    return value;
            }
        }
    }

    private static String[] convertFormat(Object value) {
        if (value == null) return null;
        if (!(value instanceof List)) throw new RuntimeException("Only array of Strings are allowed!");
        List<String> strings = (List<String>) value;
        return strings.toArray(new String[strings.size()]);
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

    public static class XLSResult {
        public long lineNo;
        public List<Object> list;
        public Map<String, Object> map;

        public XLSResult(String[] header, Object[] list, long lineNo, boolean ignore, Map<String, Mapping> mapping, List<Object> nullValues) {
            this.lineNo = lineNo;
            removeNullValues(list, nullValues);

            this.map =  createMap(header, list, ignore, mapping);
            this.list = createList(header, list, ignore, mapping);
        }

        public void removeNullValues(Object[] list, List<Object> nullValues) {
            if (nullValues.isEmpty()) return;
            for (int i = 0; i < list.length; i++) {
                if (list[i] != null && nullValues.contains(list[i])) list[i] = null;
            }
        }

        private List<Object> createList(String[] header, Object[] list, boolean ignore, Map<String, Mapping> mappings) {
            if (!ignore && mappings.isEmpty()) return asList((Object[]) list);
            ArrayList<Object> result = new ArrayList<>(list.length);
            for (int i = 0; i < header.length; i++) {
                String name = header[i];
                if (name == null) continue;
                Mapping mapping = mappings.get(name);
                if (mapping != null) {
                    if (mapping.ignore) continue;
                    result.add(mapping.convert(list[i]));
                } else {
                    result.add(list[i]);
                }
            }
            return result;
        }

        private Map<String, Object> createMap(String[] header, Object[] list, boolean ignore, Map<String, Mapping> mappings) {
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
                    map.put(mapping.name, mapping.convert(list[i]));
                }
            }
            return map;
        }
    }

}
