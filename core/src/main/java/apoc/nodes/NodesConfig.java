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
package apoc.nodes;

import apoc.util.Util;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class NodesConfig {

    public static final String MAX_DEPTH_KEY = "maxDepth";
    public static final String REL_TYPES_KEY = "relTypes";

    private final int maxDepth;
    private final List<String> relTypes;

    public NodesConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        this.maxDepth = Util.toInteger(config.getOrDefault(MAX_DEPTH_KEY, Integer.MAX_VALUE));
        this.relTypes = (List<String>) config.getOrDefault(REL_TYPES_KEY, Collections.emptyList());
    }

    public int getMaxDepth() {
        return maxDepth;
    }

    public List<String> getRelTypes() {
        return relTypes;
    }
}
