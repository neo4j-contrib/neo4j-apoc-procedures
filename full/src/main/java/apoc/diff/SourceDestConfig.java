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
package apoc.diff;

import java.util.Collections;
import java.util.Map;

public class SourceDestConfig {
    public enum SourceDestConfigType { URL, DATABASE }

    static class TargetConfig {
        private final SourceDestConfigType type;
        private final String value;

        public TargetConfig(SourceDestConfigType type, String value) {
            this.type = type;
            this.value = value;
        }

        public SourceDestConfigType getType() {
            return type;
        }

        public String getValue() {
            return value;
        }
    }

    private final Map<String, Object> params;
    private final TargetConfig target;

    public SourceDestConfig(SourceDestConfigType type, String value, Map<String, Object> params) {
        this.target = new TargetConfig(type, value);
        this.params = params;
    }

    public TargetConfig getTarget() {
        return target;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public static SourceDestConfig fromMap(Map<String, Object> map) {
        map = map == null ? Collections.emptyMap() : map;
        if (map.isEmpty()) {
            return null;
        } else {
            Map<String, Object> target = (Map<String, Object>) map.getOrDefault("target", Collections.emptyMap());
            SourceDestConfigType type = SourceDestConfigType
                    .valueOf((String) target.getOrDefault("type", SourceDestConfigType.URL.toString()));
            return new SourceDestConfig(type,
                    (String) target.get("value"),
                    (Map<String, Object>) map.getOrDefault("params", Collections.emptyMap()));
        }
    }
}
