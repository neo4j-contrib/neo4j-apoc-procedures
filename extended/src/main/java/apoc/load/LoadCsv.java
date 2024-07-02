package apoc.load;

import apoc.Extended;
import apoc.export.util.CountingReader;
import apoc.load.util.LoadCsvConfig;
import apoc.util.ExtendedUtil;
import apoc.util.FileUtils;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import apoc.load.util.Results;
import static apoc.util.ExtendedFileUtils.closeReaderSafely;
import static apoc.util.Util.cleanUrl;
import static java.util.Collections.emptyList;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

@Extended
public class LoadCsv {
    public static final String ERROR_WRONG_COL_SEPARATOR = ". Please check whether you included a delimiter before a column separator or forgot a column separator.";

    @Context
    public GraphDatabaseService db;

    @Context
    public URLAccessChecker urlAccessChecker;

    @Procedure
    @Description("apoc.load.csv('urlOrBinary',{config}) YIELD lineNo, list, map - load CSV from URL as stream of values,\n config contains any of: {skip:1,limit:5,header:false,sep:'TAB',ignore:['tmp'],nullValues:['na'],arraySep:';',mapping:{years:{type:'int',arraySep:'-',array:false,name:'age',ignore:false}}")
    public Stream<CSVResult> csv(@Name("urlOrBinary") Object urlOrBinary, @Name(value = "config", defaultValue = "{}") Map<String, Object> configMap) {
        return csvParams(urlOrBinary, null, null,configMap);
    }

    @Procedure
    @Description("apoc.load.csvParams('urlOrBinary', {httpHeader: value}, payload, {config}) YIELD lineNo, list, map - load from CSV URL (e.g. web-api) while sending headers / payload to load CSV from URL as stream of values,\n config contains any of: {skip:1,limit:5,header:false,sep:'TAB',ignore:['tmp'],nullValues:['na'],arraySep:';',mapping:{years:{type:'int',arraySep:'-',array:false,name:'age',ignore:false}}")
    public Stream<CSVResult> csvParams(@Name("urlOrBinary") Object urlOrBinary, @Name("httpHeaders") Map<String, Object> httpHeaders, @Name("payload") String payload, @Name(value = "config", defaultValue = "{}") Map<String, Object> configMap) {
        LoadCsvConfig config = new LoadCsvConfig(configMap);
        CountingReader reader = null;
        try {
            String url = null;
            if (urlOrBinary instanceof String) {
                url = (String) urlOrBinary;
                httpHeaders = httpHeaders != null ? httpHeaders : new HashMap<>();
                httpHeaders.putAll(Util.extractCredentialsIfNeeded(url, true));
            }
            reader = FileUtils.readerFor(urlOrBinary, httpHeaders, payload, config.getCompressionAlgo(), urlAccessChecker);
            return streamCsv(url, config, reader);
        } catch (Exception e) {
            closeReaderSafely(reader);
            if(!config.isFailOnError())
                return Stream.of(new CSVResult(new String[0], new String[0], 0, true, Collections.emptyMap(), emptyList(), EnumSet.noneOf(Results.class)));
            else
                throw new RuntimeException("Can't read CSV " + (urlOrBinary instanceof String ? "from URL " + cleanUrl((String) urlOrBinary) : "from binary"), e);
        }
    }

    public Stream<CSVResult> streamCsv(@Name("url") String url, LoadCsvConfig config, CountingReader reader) throws IOException {

        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setEscape(config.getEscapeChar())
                .setQuote(config.isIgnoreQuotations() ? '\0' : config.getQuoteChar())
                .setDelimiter(config.getSeparator())
                .build();

        Iterator<CSVRecord> csvIterator = csvFormat.parse(reader).iterator();

        String[] header = getHeader(csvIterator, config);
        boolean checkIgnore = !config.getIgnore().isEmpty() || config.getMappings().values().stream().anyMatch(m -> m.ignore);
        return StreamSupport.stream(new CSVSpliterator(csvIterator, header, url, config.getSkip(), config.getLimit(),
                checkIgnore, config.getMappings(), config.getNullValues(), config.getResults(), config.isIgnoreQuotations(), config.getQuoteChar(), config.isFailOnError()), false);
    }

    private static final Mapping EMPTY = new Mapping("", Collections.emptyMap(), LoadCsvConfig.DEFAULT_ARRAY_SEP, false);

    private String[] getHeader(Iterator<CSVRecord> csv, LoadCsvConfig config) throws IOException {
        if (!config.isHasHeader()) return null;
        String[] headers = csv.next().values();
        List<String> ignore = config.getIgnore();
        if (ignore.isEmpty()) return headers;

        Map<String, Mapping> mappings = config.getMappings();
        for (int i = 0; i < headers.length; i++) {
            String header = headers[i];
            if (ignore.contains(header) || mappings.getOrDefault(header, EMPTY).ignore) {
                headers[i] = null;
            }
        }
        return headers;
    }

    private static class CSVSpliterator extends Spliterators.AbstractSpliterator<CSVResult> {
        private final Iterator<CSVRecord> csv;
        private final String[] header;
        private final String url;
        private final long limit;
        private final boolean ignore;
        private final Map<String, Mapping> mapping;
        private final List<String> nullValues;
        private final EnumSet<Results> results;
        private final boolean failOnError;
        private final boolean ignoreQuotations;
        private final String quoteChar;
        long lineNo;

        public CSVSpliterator(Iterator<CSVRecord> csv, String[] header, String url, long skip, long limit, boolean ignore, Map<String, Mapping> mapping, List<String> nullValues, EnumSet<Results> results, boolean ignoreQuotations, char quoteChar, boolean failOnError) {
            super(Long.MAX_VALUE, Spliterator.ORDERED);
            this.csv = csv;
            this.header = header;
            this.url = url;
            this.ignore = ignore;
            this.mapping = mapping;
            this.nullValues = nullValues;
            this.results = results;
            this.failOnError = failOnError;
            this.limit = ExtendedUtil.isSumOutOfRange(skip, limit) ? Long.MAX_VALUE : (skip + limit);
            lineNo = skip;
            this.ignoreQuotations = ignoreQuotations;
            this.quoteChar = String.valueOf(quoteChar);
            while (skip-- > 0) {
                csv.next();
            }
        }

        @Override
        public boolean tryAdvance(Consumer<? super CSVResult> action) {
            final String message = "Error reading CSV from " + (url == null ? "binary" : " URL " + cleanUrl(url)) + " at " + lineNo;
            try {
                if (csv.hasNext() && lineNo < limit) {
                    String[] row = csv.next().values();
                    removeQuotes(row, ignoreQuotations, quoteChar);
                    action.accept(new CSVResult(header, row, lineNo, ignore,mapping, nullValues,results));
                    lineNo++;
                    return true;
                }
                return false;
            } catch (ArrayIndexOutOfBoundsException e) {
                String messageIdxOfBound = message + ERROR_WRONG_COL_SEPARATOR;
                RuntimeException exception = new RuntimeException(messageIdxOfBound);
                return skipOrFail(exception);
            }
        }

        private boolean skipOrFail(RuntimeException exception) {
            if (failOnError) {
                throw exception;
            }
            lineNo++;
            return true;
        }

        private void removeQuotes(String[] row, boolean ignoreQuotations, String quoteChar) {
            if (!ignoreQuotations) {
                return;
            }
            for (int i = 0; i < row.length; i++) {
                row[i] = row[i].replace(quoteChar, "");
            }
        }
    }
}
