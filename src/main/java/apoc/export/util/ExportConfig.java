package apoc.export.util;

import apoc.export.cypher.formatter.CypherFormat;
import apoc.gephi.Gephi;
import apoc.util.Util;

import java.util.*;

import static apoc.util.Util.toBoolean;
import static java.util.Arrays.asList;

/**
 * @author mh
 * @since 19.01.14
 */
public class ExportConfig {
    public static final char QUOTECHAR = '"';
    public static final String NONE_QUOTES = "none";
    public static final String ALWAYS_QUOTES = "always";
    public static final String IF_NEEDED_QUUOTES = "ifNeeded";

    public static final int DEFAULT_BATCH_SIZE = 20000;
    public static final String DEFAULT_DELIM = ",";
    public static final String DEFAULT_QUOTES = ALWAYS_QUOTES;
    private final boolean streamStatements;

    private int batchSize = DEFAULT_BATCH_SIZE;
    private boolean silent = false;
    private String delim = DEFAULT_DELIM;
    private String quotes = DEFAULT_QUOTES;
    private boolean useTypes = false;
    private Set<String> caption;
    private boolean writeNodeProperties = false;
    private boolean nodesOfRelationships;
    private ExportFormat format;
    private CypherFormat cypherFormat;
    private final Map<String, Object> config;

    public int getBatchSize() {
        return batchSize;
    }

    public boolean isSilent() {
        return silent;
    }

    public char getDelimChar() {
        return delim.charAt(0);
    }

    public String getDelim() {
        return delim;
    }

    public String isQuotes() {
        return quotes;
    }


    public boolean useTypes() {
        return useTypes;
    }

    public ExportFormat getFormat() { return format; }

    public Set<String> getCaption() { return caption; }

    public CypherFormat getCypherFormat() { return cypherFormat; }

    public ExportConfig(Map<String,Object> config) {
        config = config != null ? config : Collections.emptyMap();
        this.silent = toBoolean(config.getOrDefault("silent",false));
        this.batchSize = ((Number)config.getOrDefault("batchSize", DEFAULT_BATCH_SIZE)).intValue();
        this.delim = delim(config.getOrDefault("d", String.valueOf(DEFAULT_DELIM)).toString());
        this.useTypes = toBoolean(config.get("useTypes"));
        this.caption = convertCaption(config.getOrDefault("caption", asList("name", "title", "label", "id")));
        this.nodesOfRelationships = toBoolean(config.get("nodesOfRelationships"));
        this.format = ExportFormat.fromString((String) config.getOrDefault("format", "neo4j-shell"));
        this.cypherFormat = CypherFormat.fromString((String) config.getOrDefault("cypherFormat", "create"));
        this.config = config;
        this.streamStatements = toBoolean(config.get("streamStatements")) || toBoolean(config.get("stream"));
        this.writeNodeProperties = toBoolean(config.get("writeNodeProperties"));
        exportQuotes(config);
    }

    private void exportQuotes(Map<String, Object> config)
    {
        try {
            this.quotes = (String) config.getOrDefault("quotes", DEFAULT_QUOTES);

            if ( !quotes.equals(ALWAYS_QUOTES) && !quotes.equals(NONE_QUOTES) && !quotes.equals(IF_NEEDED_QUUOTES) ) {
                throw new RuntimeException("The string value of the field quote is not valid");
            }

        } catch (ClassCastException e) { // backward compatibility
            this.quotes = toBoolean(config.get("quotes")) ? ALWAYS_QUOTES : NONE_QUOTES;
        }
    }

    public boolean getRelsInBetween() {
        return nodesOfRelationships;
    }

    private static String delim(String value) {
        if (value.length()==1) return value;
        if (value.contains("\\t")) return String.valueOf('\t');
        if (value.contains(" ")) return " ";
        throw new RuntimeException("Illegal delimiter '"+value+"'");
    }

    public ExportConfig withTypes() {
        this.useTypes =true;
        return this;
    }

    public String defaultRelationshipType() {
        return config.getOrDefault("defaultRelationshipType","RELATED").toString();
    }

    public boolean readLabels() {
        return toBoolean(config.getOrDefault("readLabels",false));
    }

    public boolean storeNodeIds() {
        return toBoolean(config.getOrDefault("storeNodeIds", false));
    }

    public boolean separateFiles() {
        return toBoolean(config.getOrDefault("separateFiles", false));
    }

    private ExportFormat format(Object format) {
        return format != null && format instanceof String ? ExportFormat.fromString((String)format) : ExportFormat.NEO4J_SHELL;
    }

    private static Set<String> convertCaption(Object value) {
        if (value == null) return null;
        if (!(value instanceof List)) throw new RuntimeException("Only array of Strings are allowed!");
        List<String> strings = (List<String>) value;
        return new HashSet<>(strings);
    }

    public boolean streamStatements() {
        return streamStatements;
    }

    public boolean writeNodeProperties() {
        return writeNodeProperties;
    }

    public long getTimeoutSeconds() {
        return Util.toLong(config.getOrDefault("timeoutSeconds",100));
    }
}
