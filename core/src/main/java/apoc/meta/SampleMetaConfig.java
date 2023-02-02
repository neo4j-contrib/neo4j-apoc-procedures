
package apoc.meta;

import java.util.Collections;
import java.util.Map;

public class SampleMetaConfig {
    private final long maxRels;
    private final long sample;


    /**
     * - sample: a long, i.e. "1 in (SAMPLE)".  If set to 1000 this means that every 1000th node will be examined.
     * It does **not** mean that a total of 1000 nodes will be sampled.
     * - maxRels: the maximum number of relationships to look at per Node Label.
     */
    public SampleMetaConfig(Map<String,Object> config, Boolean shouldSampleByDefault) {
        config = config != null ? config : Collections.emptyMap();

        this.sample = (long) config.getOrDefault("sample", shouldSampleByDefault ? 1000L : 1L);
        this.maxRels = (long) config.getOrDefault("maxRels", shouldSampleByDefault ? 100L : -1L);
    }

    public SampleMetaConfig(Map<String,Object> config) {
        this(config, true);
    }
    public long getSample() {
        return sample;
    }

    public long getMaxRels() {
        return maxRels;
    }

}