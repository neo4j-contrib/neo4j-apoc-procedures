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
package apoc.util;

import java.util.HashMap;
import java.util.Map;

/**
 * @author mh
 * @since 04.05.16
 */
public final class MapUtilExtended {
    private MapUtilExtended() {}

    public static Map<String, Object> map(Object... values) {
        return UtilExtended.map(values);
    }

    public static <K, V> Map<V, K> invertMap(final Map<K, V> map) {
        final Map<V, K> out = new HashMap<>(map.size());
        for (final Map.Entry<K, V> entry : map.entrySet()) {
            out.put(entry.getValue(), entry.getKey());
        }
        return out;
    }
}