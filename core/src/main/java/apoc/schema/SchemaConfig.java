package apoc.schema;

import java.util.*;

/**
 * @author ab-larus
 * @since 17.12.18
 */
public class SchemaConfig {

    private Set<String> labels;
    private Set<String> excludeLabels;
    private Set<String> relationships;
    private Set<String> excludeRelationships;

    public Set<String> getLabels() {
        return labels;
    }

    public Set<String> getExcludeLabels() {
        return excludeLabels;
    }

    public Set<String> getRelationships() {
        return relationships;
    }

    public Set<String> getExcludeRelationships() {
        return excludeRelationships;
    }

    public SchemaConfig(Map<String,Object> config) {
        config = config != null ? config : Collections.emptyMap();
        this.labels = new HashSet<>((Collection<String>)config.getOrDefault("labels", Collections.EMPTY_SET));
        this.excludeLabels = new HashSet<>((Collection<String>) config.getOrDefault("excludeLabels", Collections.EMPTY_SET));
        validateParameters(this.labels, this.excludeLabels, "labels");
        this.relationships = new HashSet<>((Collection<String>)config.getOrDefault("relationships", Collections.EMPTY_SET));
        this.excludeRelationships = new HashSet<>((Collection<String>)config.getOrDefault("excludeRelationships", Collections.EMPTY_SET));
        validateParameters(this.labels, this.excludeLabels, "relationships");
    }

    private void validateParameters(Set<String> include, Set<String> exclude, String parametrType){
        if(!include.isEmpty() && !exclude.isEmpty())
            throw new IllegalArgumentException(String.format("Parameters %s and exclude%s are both valuated. Please check parameters and valuate only one.", parametrType, parametrType));
    }
}
