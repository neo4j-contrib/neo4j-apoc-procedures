package apoc.export.util;

import java.util.Collections;
import java.util.Map;

import static apoc.util.Util.toBoolean;

/**
 * @author mh
 * @since 19.01.14
 */
public class ExportConfig {
    public static final char QUOTECHAR = '"';
    public static final int DEFAULT_BATCH_SIZE = 20000;
    public static final String DEFAULT_DELIM = ",";

    private int batchSize = DEFAULT_BATCH_SIZE;
    private boolean silent = false;
    private String delim = DEFAULT_DELIM;
    private boolean quotes;
    private boolean useTypes = false;
    private boolean nodesOfRelationships;
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

    public boolean isQuotes() {
        return quotes;
    }

    public boolean useTypes() {
        return useTypes;
    }


    public ExportConfig(Map<String,Object> config) {
        config = config != null ? config : Collections.emptyMap();
        this.silent = toBoolean(config.getOrDefault("silent",false));
        this.batchSize = ((Number)config.getOrDefault("batchSize", DEFAULT_BATCH_SIZE)).intValue();
        this.delim = delim(config.getOrDefault("d", String.valueOf(DEFAULT_DELIM)).toString());
        this.quotes = toBoolean(config.get("quotes"));
        this.useTypes = toBoolean(config.get("useTypes"));
        this.nodesOfRelationships = toBoolean(config.get("nodesOfRelationships"));
        this.config = config;
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
        return toBoolean(config.getOrDefault("storeNodeIds",false));
    }
}
