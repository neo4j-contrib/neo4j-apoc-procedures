package apoc.load;

import apoc.export.util.CountingReader;
import apoc.load.util.LoadCsvConfig;
import apoc.meta.Meta;
import apoc.util.FileUtils;
import apoc.util.Util;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
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

import static apoc.load.util.LoadCsvConfig.DEFAULT_ARRAY_SEP;
import static apoc.load.util.LoadCsvConfig.Results;
import static apoc.util.FileUtils.closeReaderSafely;
import static apoc.util.Util.cleanUrl;
import static apoc.util.Util.parseCharFromConfig;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class LoadCsv {


    @Context
    public GraphDatabaseService db;

    @Procedure
    @Description("apoc.load.csv('url',{config}) YIELD lineNo, list, map - load CSV fom URL as stream of values,\n config contains any of: {skip:1,limit:5,header:false,sep:'TAB',ignore:['tmp'],nullValues:['na'],arraySep:';',mapping:{years:{type:'int',arraySep:'-',array:false,name:'age',ignore:false}}")
    public Stream<CSVResult> csv(@Name("url") String url, @Name(value = "config",defaultValue = "{}") Map<String, Object> configMap) {
        LoadCsvConfig config = new LoadCsvConfig(configMap);
        CountingReader reader = null;
        try {
            reader = FileUtils.readerFor(url);
            return streamCsv(url, config, reader);
        } catch (IOException e) {
            closeReaderSafely(reader);
            if(!config.isFailOnError())
                return Stream.of(new CSVResult(new String[0], new String[0], 0, true, Collections.emptyMap(), emptyList(), EnumSet.noneOf(Results.class)));
            else
                throw new RuntimeException("Can't read CSV from URL " + cleanUrl(url), e);
        }
    }

    public Stream<CSVResult> streamCsv(@Name("url") String url, LoadCsvConfig config, CountingReader reader) throws IOException {

        CSVReader csv = new CSVReaderBuilder(reader)
                .withCSVParser(new CSVParserBuilder()
                        .withQuoteChar(config.getQuoteChar())
                        .withSeparator(config.getSeparator())
                        .build())
                .build();

        String[] header = getHeader(csv, config);
        boolean checkIgnore = !config.getIgnore().isEmpty() || config.getMappings().values().stream().anyMatch(m -> m.ignore);
        return StreamSupport.stream(new CSVSpliterator(csv, header, url, config.getSkip(), config.getLimit(),
                checkIgnore, config.getMappings(), config.getNullValues(), config.getResults(), config.getIgnoreErrors()), false)
                .onClose(() -> closeReaderSafely(reader));
    }

    public static class Mapping {
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

    private String[] getHeader(CSVReader csv, LoadCsvConfig config) throws IOException {
        if (!config.isHasHeader()) return null;
        String[] headers = csv.readNext();
        List<String> ignore = config.getIgnore();
        if (ignore.isEmpty()) return headers;

        Map<String, Mapping> mappings = config.getMappings();
        for (int i = 0; i < headers.length; i++) {
            String header = headers[i];
            if (ignore.contains(header) || mappings.getOrDefault(header, Mapping.EMPTY).ignore) {
                headers[i] = null;
            }
        }
        return headers;
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
        private final boolean ignoreErrors;
        long lineNo;

        public CSVSpliterator(CSVReader csv, String[] header, String url, long skip, long limit, boolean ignore, Map<String, Mapping> mapping, List<String> nullValues, EnumSet<Results> results, boolean ignoreErrors) throws IOException {
            super(Long.MAX_VALUE, Spliterator.ORDERED);
            this.csv = csv;
            this.header = header;
            this.url = url;
            this.ignore = ignore;
            this.mapping = mapping;
            this.nullValues = nullValues;
            this.results = results;
            this.ignoreErrors = ignoreErrors;
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
                    action.accept(new CSVResult(header, row, lineNo, ignore,mapping, nullValues,results));
                    lineNo++;
                    return true;
                }
                return false;
            } catch (IOException e) {
                throw new RuntimeException("Error reading CSV from URL " + cleanUrl(url) + " at " + lineNo, e);
            }
        }
    }
}
