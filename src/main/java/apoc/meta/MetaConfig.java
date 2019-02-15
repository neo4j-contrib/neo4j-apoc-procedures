package apoc.meta;

import java.util.*;

public class MetaConfig {

    private Set<String> includesLabels;
    private Set<String> includesRels;
    private Set<String> excludes;
    private long maxRels;
    private long sample;

    public MetaConfig(Map<String,Object> config) {
        config = config != null ? config : Collections.emptyMap();
        this.includesLabels = new HashSet<>((Collection<String>)config.getOrDefault("labels",Collections.EMPTY_SET));
        this.includesRels = new HashSet<>((Collection<String>)config.getOrDefault("rels",Collections.EMPTY_SET));
        this.excludes = new HashSet<>((Collection<String>)config.getOrDefault("excludes",Collections.EMPTY_SET));
        this.sample = (long) config.getOrDefault("sample", 1000L);
        this.maxRels = (long) config.getOrDefault("maxRels", 100L);
    }

    public Set<String> getIncludesLabels() {
        return includesLabels;
    }

    public Set<String> getIncludesRels() {
        return includesRels;
    }

    public Set<String> getExcludes() {
        return excludes;
    }

    public void setExcludes(Set<String> excludes) {
        this.excludes = excludes;
    }

    public long getSample() {
        return sample;
    }


    public long getMaxRels() {
        return maxRels;
    }
}
