package apoc.export.xls;

import apoc.util.Util;

import java.util.Collections;
import java.util.Map;

public class XlsExportConfig {

    public static final int DEFAULT_BATCH_SIZE = 20000;

    private final int batchSize;
    private final Map<String, Object> config;
    private final String headerNodeId;
    private final String headerRelationshipId;
    private final String headerStartNodeId;
    private final String headerEndNodeId;
    private final String arrayDelimiter;
    private final String dateTimeStyle;
    private final String dateStyle;
    private final boolean prefixSheetWithEntityType;
    private final boolean joinLabels;

    public XlsExportConfig(Map<String,Object> c) {
        config = c != null ? c : Collections.emptyMap();
        this.headerNodeId = (String) config.getOrDefault("headerNodeId", "<nodeId>");
        this.headerRelationshipId= (String) config.getOrDefault("headerRelationshipId", "<relationshipId>");
        this.headerStartNodeId = (String) config.getOrDefault("headerStartNodeId", "<startNodeId>");
        this.headerEndNodeId = (String) config.getOrDefault("headerEndNodeId", "<endNodeId>");
        this.arrayDelimiter = (String) config.getOrDefault("arrayDelimiter", ";");
        this.dateTimeStyle = (String) config.getOrDefault("dateTimeStyle", "yyyy-mm-dd hh:mm:ss");
        this.dateStyle = (String) config.getOrDefault("dateStyle", "yyyy-mm-dd");
        this.batchSize = ((Number)config.getOrDefault("batchSize", DEFAULT_BATCH_SIZE)).intValue();
        this.prefixSheetWithEntityType = Util.toBoolean(config.getOrDefault("prefixSheetWithEntityType", false));
        this.joinLabels = Util.toBoolean(config.getOrDefault("joinLabels", false));
    }

    public String getHeaderNodeId() {
        return headerNodeId;
    }

    public String getHeaderRelationshipId() {
        return headerRelationshipId;
    }

    public String getHeaderStartNodeId() {
        return headerStartNodeId;
    }

    public String getHeaderEndNodeId() {
        return headerEndNodeId;
    }

    public String getArrayDelimiter() {
        return arrayDelimiter;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public String getDateTimeStyle() {
        return dateTimeStyle;
    }

    public String getDateStyle() {
        return dateStyle;
    }

    public boolean isPrefixSheetWithEntityType() {
        return prefixSheetWithEntityType;
    }

    public boolean isJoinLabels() {
        return joinLabels;
    }
}
