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
package apoc.hashing;

import apoc.util.Util;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FingerprintingConfig {

    enum FingerprintStrategy {EAGER, LAZY}

    private final String digestAlgorithm;
    private final Map<String, List<String>> nodeAllowMap;
    private final Map<String, List<String>> relAllowMap;

    private final Map<String, List<String>> nodeDisallowMap;
    private final Map<String, List<String>> relDisallowMap;

    private final List<String> mapAllowList;
    private final List<String> mapDisallowList;
    private final List<String> allNodesAllowList;
    private final List<String> allRelsAllowList;
    private final List<String> allNodesDisallowList;
    private final List<String> allRelsDisallowList;
    private final FingerprintStrategy strategy;
    private final Set<String> allLabels;
    private final Set<String> allTypes;


    public FingerprintingConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        this.digestAlgorithm = (String) config.getOrDefault("digestAlgorithm", "MD5");

        this.nodeAllowMap = (Map<String, List<String>>) config.getOrDefault("nodeAllowMap", Collections.emptyMap());
        this.relAllowMap = (Map<String, List<String>>) config.getOrDefault("relAllowMap", Collections.emptyMap());
        this.nodeDisallowMap = (Map<String, List<String>>) config.getOrDefault("nodeDisallowMap", Collections.emptyMap());
        this.relDisallowMap = (Map<String, List<String>>) config.getOrDefault("relDisallowMap", Collections.emptyMap());
        this.mapAllowList = (List<String>) config.getOrDefault("mapAllowList", Collections.emptyList());
        this.mapDisallowList = (List<String>) config.getOrDefault("mapDisallowList", Collections.emptyList());
        this.allNodesAllowList = (List<String>) config.getOrDefault("allNodesAllowList", Collections.emptyList());
        this.allRelsAllowList = (List<String>) config.getOrDefault("allRelsAllowList", Collections.emptyList());
        this.allNodesDisallowList = (List<String>) config.getOrDefault("allNodesDisallowList", Collections.emptyList());
        this.allRelsDisallowList = (List<String>) config.getOrDefault("allRelsDisallowList", Collections.emptyList());
        this.strategy = FingerprintStrategy.valueOf((String) config.getOrDefault("strategy", FingerprintStrategy.LAZY.toString()));

        validateConfig();

        allLabels = new HashSet<>(nodeAllowMap.keySet());
        allLabels.addAll(nodeDisallowMap.keySet());
        allTypes = new HashSet<>(relAllowMap.keySet());
        allTypes.addAll(relDisallowMap.keySet());
    }

    private void validateConfig() {
        final String message = "You can't set the same %s for allow and disallow lists for %s";
        if (!Util.intersection(nodeAllowMap.keySet(), nodeDisallowMap.keySet()).isEmpty()) {
            throw new RuntimeException(String.format(message, "labels", "nodes"));
        }
        if (!Util.intersection(relAllowMap.keySet(), relDisallowMap.keySet()).isEmpty()) {
            throw new RuntimeException(String.format(message, "types", "rels"));
        }
        if (!Util.intersection(mapAllowList, mapDisallowList).isEmpty()) {
            throw new RuntimeException(String.format(message, "properties", "maps"));
        }
        if (!Util.intersection(allNodesAllowList, allNodesDisallowList).isEmpty()) {
            throw new RuntimeException(String.format(message, "properties", "all nodes"));
        }
        if (!Util.intersection(allRelsAllowList, allRelsDisallowList).isEmpty()) {
            throw new RuntimeException(String.format(message, "properties", "all rels"));
        }
    }

    public String getDigestAlgorithm() {
        return digestAlgorithm;
    }

    public Map<String, List<String>> getNodeAllowMap() {
        return nodeAllowMap;
    }

    public Map<String, List<String>> getRelAllowMap() {
        return relAllowMap;
    }

    public Map<String, List<String>> getNodeDisallowMap() {
        return nodeDisallowMap;
    }

    public Map<String, List<String>> getRelDisallowMap() {
        return relDisallowMap;
    }

    public List<String> getMapAllowList() {
        return mapAllowList;
    }

    public List<String> getMapDisallowList() {
        return mapDisallowList;
    }

    public List<String> getAllNodesAllowList() {
        return allNodesAllowList;
    }

    public List<String> getAllRelsAllowList() {
        return allRelsAllowList;
    }

    public List<String> getAllNodesDisallowList() {
        return allNodesDisallowList;
    }

    public List<String> getAllRelsDisallowList() {
        return allRelsDisallowList;
    }

    public FingerprintStrategy getStrategy() {
        return strategy;
    }

    public Set<String> getAllLabels() {
        return allLabels;
    }

    public Set<String> getAllTypes() {
        return allTypes;
    }
}