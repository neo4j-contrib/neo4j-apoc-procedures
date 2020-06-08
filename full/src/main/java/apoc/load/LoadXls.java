package apoc.load;

import apoc.Extended;
import apoc.export.util.CountingInputStream;
import apoc.meta.Meta;
import apoc.util.FileUtils;
import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.storable.LocalDateTimeValue;

import java.io.IOException;
import java.time.*;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.util.DateParseUtil.dateParse;
import static apoc.util.Util.*;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

@Extended
public class LoadXls {

    public static final char DEFAULT_ARRAY_SEP = ';';
    @Context
    public GraphDatabaseService db;

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
        try (CountingInputStream stream = FileUtils.inputStreamFor(url)) {
            Selection selection = new Selection(selector);

            char arraySep = separator(config, "arraySep", DEFAULT_ARRAY_SEP);
            long skip = longValue(config, "skip", 0L);
            boolean hasHeader = booleanValue(config, "header", true);
            long limit = longValue(config, "limit", Long.MAX_VALUE);

            List<String> ignore = value(config, "ignore", emptyList());
            List<Object> nullValues = value(config, "nullValues", emptyList());
            Map<String, Map<String, Object>> mapping = value(config, "mapping", Collections.emptyMap());
            Map<String, Mapping> mappings = createMapping(mapping, arraySep, ignore);

            Workbook workbook = WorkbookFactory.create(stream);
            Sheet sheet = workbook.getSheet(selection.sheet);
            if (sheet==null) throw new IllegalStateException("Sheet "+selection.sheet+" not found");
            selection.updateVertical(sheet.getFirstRowNum(),sheet.getLastRowNum());
            Row firstRow = sheet.getRow(selection.top);
            selection.updateHorizontal(firstRow.getFirstCellNum(), firstRow.getLastCellNum());

            String[] header = getHeader(hasHeader, firstRow,selection, ignore, mappings);
            boolean checkIgnore = !ignore.isEmpty() || mappings.values().stream().anyMatch( m -> m.ignore);
            return StreamSupport.stream(new XLSSpliterator(sheet, selection, header, url, skip, limit, checkIgnore,mappings, nullValues), false);
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
        final Meta.Types type;
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
            this.type = Meta.Types.from(mapping.getOrDefault("type", "STRING").toString());
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

    private String[] getHeader(boolean hasHeader, Row header, Selection selection, List<String> ignore, Map<String, Mapping> mapping) throws IOException {
        if (!hasHeader) return null;

        String[] result = new String[selection.right - selection.left];
        for (int i = selection.left; i < selection.right; i++) {
            Cell cell = header.getCell(i);
            if (cell == null) throw new IllegalStateException("Header at position "+i+" doesn't have a value");
            String value = cell.getStringCellValue();
            result[i- selection.left] = ignore.contains(value) || mapping.getOrDefault(value, Mapping.EMPTY).ignore ? null : value;
        }
        return result;
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

    private static Object[] extract(Row row, Selection selection) {
        // selection.updateHorizontal(row.getFirstCellNum(),row.getLastCellNum());
        Object[] result = new Object[selection.right-selection.left];
        for (int i = selection.left; i < selection.right; i++) {
            Cell cell = row.getCell(i);
            if (cell == null) continue;
            result[i-selection.left] = getValue(cell, cell.getCellTypeEnum());
        }
        return result;

    }

    private static Object getValue(Cell cell, CellType type) {
        switch (type) {
            case NUMERIC: // In excel the date is NUMERIC Type
                if (DateUtil.isCellDateFormatted(cell)) {
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(cell.getDateCellValue());
                    LocalDateTime localDateTime = LocalDateTime.of(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH)+1, cal.get(Calendar.DAY_OF_MONTH),
                            cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND));
                    return LocalDateTimeValue.localDateTime(localDateTime);
//                    return LocalDateTimeValue.localDateTime(cell.getDateCellValue().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime());
                }
                double value = cell.getNumericCellValue();
                if (value == Math.floor(value)) return (long) value;
                return value;
            case STRING: return cell.getStringCellValue();
            case FORMULA: return getValue(cell,cell.getCachedFormulaResultTypeEnum());
            case BOOLEAN: return cell.getBooleanCellValue();
            case _NONE: return null;
            case BLANK: return null;
            case ERROR: return null;
            default: return null;
        }
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

    private static class XLSSpliterator extends Spliterators.AbstractSpliterator<XLSResult> {
        private final Sheet sheet;
        private final Selection selection;
        private final String[] header;
        private final String url;
        private final long limit;
        private final boolean ignore;
        private final Map<String, Mapping> mapping;
        private final List<Object> nullValues;
        private final long skip;
        long lineNo;

        public XLSSpliterator(Sheet sheet, Selection selection, String[] header, String url, long skip, long limit, boolean ignore, Map<String, Mapping> mapping, List<Object> nullValues) throws IOException {
            super(Long.MAX_VALUE, Spliterator.ORDERED);
            this.sheet = sheet;
            this.selection = selection;
            this.header = header;
            this.url = url;
            this.ignore = ignore;
            this.mapping = mapping;
            this.nullValues = nullValues;
            int headerOffset = header != null ? 1 : 0;
            this.skip = skip + selection.getOrDefault(selection.top, sheet.getFirstRowNum()) + headerOffset;
            this.limit = limit == Long.MAX_VALUE ? selection.getOrDefault(selection.bottom, sheet.getLastRowNum()) : skip + limit;
            lineNo = this.skip;
        }

        @Override
        public boolean tryAdvance(Consumer<? super XLSResult> action) {
            try {
                Row row = sheet.getRow((int)lineNo);
                if (row != null && lineNo <= limit) {
                    Object[] list = extract(row, selection);
                    action.accept(new XLSResult(header, list, lineNo-skip, ignore,mapping, nullValues));
                    lineNo++;
                    return true;
                }
                return false;
            } catch (Exception e) {
                throw new RuntimeException("Error reading XLS from URL " + cleanUrl(url) + " at " + lineNo, e);
            }
        }
    }
}
