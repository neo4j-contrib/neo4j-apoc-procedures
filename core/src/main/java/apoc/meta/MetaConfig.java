/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.meta;

import apoc.util.Util;
import java.util.*;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class MetaConfig {

    private final Set<String> includeLabels;
    private final Set<String> includeRels;
    private final Set<String> excludeLabels;
    private final Set<String> excludeRels;
    private final boolean addRelationshipsBetweenNodes;

    private final SampleMetaConfig sampleMetaConfig;

    /**
     * A map of values, with the following keys and meanings.
     * - includeLabels: a list of strings, which are allowlisted node labels. If this list
     * is specified **only these labels** will be examined.
     * - includeRels: a list of strings, which are allowlisted rel types.  If this list is
     * specified, **only these reltypes** will be examined.
     * - excludeLabels: a list of strings, which are node labels.  This
     * works like a denylist: if listed here, the thing won't be considered.  Everything
     * else (subject to the allowlist) will be.
     * - excludeRels: a list of strings, which are relationship types.  This
     * works like a denylist: if listed here, the thing won't be considered.  Everything
     * else (subject to the allowlist) will be.
     * - sample: a long number, i.e. "1 in (SAMPLE)".  If set to 1000 this means that
     * every 1000th node will be examined.  It does **not** mean that a total of 1000 nodes
     * will be sampled.
     * - maxRels: the maximum number of relationships to look at per Node Label.
     */
    public MetaConfig(Map<String, Object> config, Boolean shouldSampleByDefault) {
        config = config != null ? config : Collections.emptyMap();

        // TODO: Remove in 6.0: To maintain backwards compatibility until then we still need to support;
        // "labels", "rels" and "excludes" for "includeLabels", "includeRels" and "excludeLabels" respectively.

        Set<String> includesLabelsLocal =
                new HashSet<>((Collection<String>) config.getOrDefault("labels", Collections.EMPTY_SET));
        Set<String> includesRelsLocal =
                new HashSet<>((Collection<String>) config.getOrDefault("rels", Collections.EMPTY_SET));
        Set<String> excludesLocal =
                new HashSet<>((Collection<String>) config.getOrDefault("excludes", Collections.EMPTY_SET));

        if (includesLabelsLocal.isEmpty()) {
            includesLabelsLocal =
                    new HashSet<>((Collection<String>) config.getOrDefault("includeLabels", Collections.EMPTY_SET));
        }
        if (includesRelsLocal.isEmpty()) {
            includesRelsLocal =
                    new HashSet<>((Collection<String>) config.getOrDefault("includeRels", Collections.EMPTY_SET));
        }
        if (excludesLocal.isEmpty()) {
            excludesLocal =
                    new HashSet<>((Collection<String>) config.getOrDefault("excludeLabels", Collections.EMPTY_SET));
        }

        this.includeLabels = includesLabelsLocal;
        this.includeRels = includesRelsLocal;
        this.excludeLabels = excludesLocal;
        this.excludeRels =
                new HashSet<>((Collection<String>) config.getOrDefault("excludeRels", Collections.EMPTY_SET));
        this.sampleMetaConfig = new SampleMetaConfig(config, shouldSampleByDefault);
        this.addRelationshipsBetweenNodes = Util.toBoolean(config.getOrDefault("addRelationshipsBetweenNodes", true));
    }

    public MetaConfig(Map<String, Object> config) {
        this(config, true);
    }

    public Set<String> getIncludeLabels() {
        return includeLabels;
    }

    public Set<String> getIncludeRels() {
        return includeRels;
    }

    public Set<String> getExcludeLabels() {
        return excludeLabels;
    }

    public Set<String> getExcludeRels() {
        return excludeRels;
    }

    public long getSample() {
        return sampleMetaConfig.getSample();
    }

    public long getMaxRels() {
        return sampleMetaConfig.getMaxRels();
    }

    public SampleMetaConfig getSampleMetaConfig() {
        return sampleMetaConfig;
    }

    /**
     * @param l
     * @return true if the label matches the mask expressed by this object, false otherwise.
     */
    public boolean matches(Label l) {
        if (getExcludeLabels().contains(l.name())) {
            return false;
        }
        if (getIncludeLabels().isEmpty()) {
            return true;
        }
        return getIncludeLabels().contains(l.name());
    }

    /**
     * @param labels
     * @return true if any of the labels matches the mask expressed by this object, false otherwise.
     */
    public boolean matches(Iterable<Label> labels) {
        // If it matches any label, it gets looked at, because labels can co-occur.
        boolean match = true;

        for (Label l : labels) {
            match = match || matches(l);
        }

        return match;
    }

    /**
     * @param r
     * @return true if the relationship matches the mask expressed by this object, false otherwise.
     */
    public boolean matches(Relationship r) {
        return matches(r.getType());
    }

    /**
     * @param rt
     * @return true if the relationship type matches the mask expressed by this object, false otherwise.
     */
    public boolean matches(RelationshipType rt) {
        String name = rt.name();

        if (getExcludeRels().contains(name)) {
            return false;
        }
        if (getIncludeRels().isEmpty()) {
            return true;
        }
        return getIncludeRels().contains(name);
    }

    public boolean isAddRelationshipsBetweenNodes() {
        return addRelationshipsBetweenNodes;
    }
}
