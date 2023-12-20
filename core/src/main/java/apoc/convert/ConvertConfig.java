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
package apoc.convert;

import apoc.util.Util;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author AgileLARUS
 *
 * @since 28-01-2019
 */
public class ConvertConfig {

    private Map<String, List<String>> nodes;
    private Map<String, List<String>> rels;
    private final boolean sortPaths;

    public ConvertConfig(Map<String, Object> config) {
        this.sortPaths = Util.toBoolean(config.getOrDefault("sortPaths", true));

        this.nodes = (Map<String, List<String>>) config.getOrDefault("nodes", Collections.EMPTY_MAP);
        this.rels = (Map<String, List<String>>) config.getOrDefault("rels", Collections.EMPTY_MAP);

        this.nodes.values().forEach(s -> validateListProperties(s));
        this.rels.values().forEach(s -> validateListProperties(s));
    }

    public Map<String, List<String>> getNodes() {
        return nodes;
    }

    public Map<String, List<String>> getRels() {
        return rels;
    }

    public boolean isSortPaths() {
        return sortPaths;
    }

    private void validateListProperties(List<String> list) {
        boolean isFirstExclude = list.get(0).startsWith("-");
        Optional<String> hasMixedProp = list.stream()
                .skip(1)
                .filter(prop -> (isFirstExclude && !prop.startsWith("-")) || (!isFirstExclude && prop.startsWith("-")))
                .findFirst();
        if (hasMixedProp.isPresent()) {
            throw new RuntimeException("Only include or exclude attribute are possible!");
        }
    }
}
