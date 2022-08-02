package apoc.export.json;

import apoc.util.CompressionConfig;
import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ImportJsonConfig extends CompressionConfig {
    public static final String WILDCARD_PROPS = "_all";
    private final Map<String, List<String>> nodePropFilter;
    private final Map<String, List<String>> relPropFilter;

    private final Map<String, Map<String, String>> nodePropertyMappings;
    private final Map<String, Map<String, String>> relPropertyMappings;

    private final int unwindBatchSize;
    private final int txBatchSize;

    private final String importIdName;
    
    private final boolean cleanup;

    public ImportJsonConfig(Map<String, Object> config) {
        super(config);
        config = config == null ? Collections.emptyMap() : config;
        this.nodePropertyMappings = (Map<String, Map<String, String>>) config.getOrDefault("nodePropertyMappings", Collections.emptyMap());
        this.relPropertyMappings = (Map<String, Map<String, String>>) config.getOrDefault("relPropertyMappings", Collections.emptyMap());
        this.unwindBatchSize = Util.toInteger(config.getOrDefault("unwindBatchSize", 5000));
        this.txBatchSize = Util.toInteger(config.getOrDefault("txBatchSize", 5000));
        this.importIdName = (String) config.getOrDefault("importIdName", "neo4jImportId");
        this.cleanup = Util.toBoolean(config.get("cleanup"));
        this.nodePropFilter = (Map<String, List<String>>) config.getOrDefault("nodePropFilter", Collections.emptyMap());
        this.relPropFilter = (Map<String, List<String>>) config.getOrDefault("relPropFilter", Collections.emptyMap());
    }

    public String typeForNode(Collection<String> labels, String property) {
        return labels.stream()
                .map(label -> nodePropertyMappings.getOrDefault(label, Collections.emptyMap()).get(property))
                .filter(StringUtils::isNotBlank)
                .findFirst()
                .orElse(null);
    }

    public String typeForRel(String type, String property) {
        return relPropertyMappings.getOrDefault(type, Collections.emptyMap()).get(property);
    }

    public int getUnwindBatchSize() {
        return unwindBatchSize;
    }

    public int getTxBatchSize() {
        return txBatchSize;
    }

    public String getImportIdName() {
        return importIdName;
    }

    public boolean isCleanup() {
        return cleanup;
    }

    public Map<String, List<String>> getNodePropFilter() {
        return nodePropFilter;
    }

    public Map<String, List<String>> getRelPropFilter() {
        return relPropFilter;
    }
}
