package apoc.export.json;

import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class ImportJsonConfig {

    private final Map<String, Map<String, String>> nodePropertyMappings;
    private final Map<String, Map<String, String>> relPropertyMappings;

    private final int unwindBatchSize;
    private final int txBatchSize;

    private final String importIdName;

    public ImportJsonConfig(Map<String, Object> config) {
        config = config == null ? Collections.emptyMap() : config;
        this.nodePropertyMappings = (Map<String, Map<String, String>>) config.getOrDefault("nodePropertyMappings", Collections.emptyMap());
        this.relPropertyMappings = (Map<String, Map<String, String>>) config.getOrDefault("relPropertyMappings", Collections.emptyMap());
        this.unwindBatchSize = Util.toInteger(config.getOrDefault("unwindBatchSize", 5000));
        this.txBatchSize = Util.toInteger(config.getOrDefault("txBatchSize", 5000));
        this.importIdName = (String) config.getOrDefault("importIdName", "neo4jImportId");
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
}
