package apoc.meta;

import java.util.*;

public class MetaConfig {

    private Set<String> includesLabels;
    private Set<String> includesRels;
    private Set<String> excludes;
    private long maxRels;
    private long sample;

    /**
     * A map of values, with the following keys and meanings.
     * - labels: a list of strings, which are whitelisted node labels. If this list
     * is specified **only these labels** will be examined.
     * - rels: a list of strings, which are whitelisted rel types.  If this list is
     * specified, **only these reltypes** will be examined.
     * - excludes: a list of strings, which can be either node labels or reltypes.  This
     * works like a blacklist: if listed here, the thing won't be considered.  Everything
     * else (subject to the whitelist) will be.
     * - sample: a long number, i.e. "1 in (SAMPLE)".  If set to 1000 this means that
     * every 1000th node will be examined.  It does **not** mean that a total of 1000 nodes
     * will be sampled.
     * - maxRels: the maximum number of relationships of a given type to look at.
     * @param config
     */
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
