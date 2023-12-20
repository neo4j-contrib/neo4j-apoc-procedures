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
package apoc.trigger;

import apoc.SystemPropertyKeys;
import apoc.util.Util;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.graphdb.Node;

public class TriggerInfo {
    public String name;
    public String query;
    public Map<String, Object> selector;
    public Map<String, Object> params;
    public boolean installed = false;
    public boolean paused = false;

    public TriggerInfo(String name) {
        this.name = name;
    }

    public TriggerInfo(String name, String query) {
        this(name);
        this.query = query;
    }

    public TriggerInfo(String name, String query, Map<String, Object> selector, boolean installed, boolean paused) {
        this(name, query);
        this.selector = selector;
        this.installed = installed;
        this.paused = paused;
    }

    public TriggerInfo(
            String name,
            String query,
            Map<String, Object> selector,
            Map<String, Object> params,
            boolean installed,
            boolean paused) {
        this(name, query, selector, installed, paused);
        this.params = params;
    }

    public static TriggerInfo from(Map<String, Object> mapInfo, boolean installed, String name) {
        return new TriggerInfo(
                name,
                (String) mapInfo.get(SystemPropertyKeys.statement.name()),
                (Map<String, Object>) mapInfo.get(SystemPropertyKeys.selector.name()),
                (Map<String, Object>) mapInfo.get(SystemPropertyKeys.params.name()),
                installed,
                (boolean) mapInfo.getOrDefault(SystemPropertyKeys.paused.name(), true));
    }

    public static TriggerInfo from(Map<String, Object> mapInfo, boolean installed) {
        return from(mapInfo, installed, (String) mapInfo.get(SystemPropertyKeys.name.name()));
    }

    public static TriggerInfo fromNode(Node node, boolean installed) {
        // filter and transform node props to map
        final Map<String, Object> triggerMap = toTriggerMap(node);

        // transform map to TriggerInfo
        return from(triggerMap, installed);
    }

    private static Map<String, Object> toTriggerMap(Node node) {
        return node.getAllProperties().entrySet().stream()
                .filter(e -> !SystemPropertyKeys.database.name().equals(e.getKey()))
                .collect(
                        HashMap::new, // workaround for https://bugs.openjdk.java.net/browse/JDK-8148463
                        (mapAccumulator, e) -> {
                            Object value = List.of(SystemPropertyKeys.selector.name(), SystemPropertyKeys.params.name())
                                            .contains(e.getKey())
                                    ? Util.fromJson((String) e.getValue(), Map.class)
                                    : e.getValue();

                            mapAccumulator.put(e.getKey(), value);
                        },
                        HashMap::putAll);
    }
}
