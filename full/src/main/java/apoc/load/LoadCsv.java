package apoc.load;

import apoc.Extended;
import apoc.export.util.CountingReader;
import apoc.load.util.LoadCsvConfig;
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
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import apoc.load.util.Results;
import static apoc.util.FileUtils.closeReaderSafely;
import static apoc.util.Util.cleanUrl;
import static java.util.Collections.emptyList;

@Extended
public class LoadCsv {

    @Context
    public GraphDatabaseService db;

    @Procedure
    @Description("apoc.load.csv('url',{config}) YIELD lineNo, list, map - load CSV from URL as stream of values,\n config contains any of: {skip:1,limit:5,header:false,sep:'TAB',ignore:['tmp'],nullValues:['na'],arraySep:';',mapping:{years:{type:'int',arraySep:'-',array:false,name:'age',ignore:false}}")
    public Stream<CSVResult> csv(@Name("url") String url, @Name(value = "config", defaultValue = "{}") Map<String, Object> configMap) {
        return csvParams(url, null, null,configMap);
    }

    @Procedure
    @Description("apoc.load.csvParams('url', {httpHeader: value}, payload, {config}) YIELD lineNo, list, map - load from CSV URL (e.g. web-api) while sending headers / payload to load CSV from URL as stream of values,\n config contains any of: {skip:1,limit:5,header:false,sep:'TAB',ignore:['tmp'],nullValues:['na'],arraySep:';',mapping:{years:{type:'int',arraySep:'-',array:false,name:'age',ignore:false}}")
    public Stream<CSVResult> csvParams(@Name("url") String url, @Name("httpHeaders") Map<String, Object> httpHeaders, @Name("payload") String payload, @Name(value = "config", defaultValue = "{}") Map<String, Object> configMap) {
        LoadCsvConfig config = new LoadCsvConfig(configMap);
        CountingReader reader = null;
        try {
            httpHeaders = httpHeaders != null ? httpHeaders : new HashMap<>();
            httpHeaders.putAll(Util.extractCredentialsIfNeeded(url, true));
            reader = FileUtils.readerFor(url, httpHeaders, payload);
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
                        .withIgnoreQuotations( config.isIgnoreQuotations() )
                        .withSeparator(config.getSeparator())
                        .build())
                .build();

        String[] header = getHeader(csv, config);
        boolean checkIgnore = !config.getIgnore().isEmpty() || config.getMappings().values().stream().anyMatch(m -> m.ignore);
        return StreamSupport.stream(new CSVSpliterator(csv, header, url, config.getSkip(), config.getLimit(),
                checkIgnore, config.getMappings(), config.getNullValues(), config.getResults(), config.getIgnoreErrors()), false)
                .onClose(() -> closeReaderSafely(reader));
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
            this.limit = Util.isSumOutOfRange(skip, limit) ? Long.MAX_VALUE : (skip + limit);
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
