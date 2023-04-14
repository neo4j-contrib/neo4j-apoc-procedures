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

import apoc.util.Util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DiffConfig {
    private final boolean findById;
    private final boolean relsInBetween;
    private final Map<String, Object> boltConfig;

    public DiffConfig(Map<String, Object> config) {
        if (config == null) {
            config = Collections.emptyMap();
        }
        this.findById = Util.toBoolean(config.get("findById"));
        this.relsInBetween = Util.toBoolean(config.get("relsInBetween"));
        this.boltConfig = (Map<String, Object>) config.getOrDefault("boltConfig", new HashMap<>());
    }

    public boolean isFindById() {
        return findById;
    }

    public Map<String, Object> getBoltConfig() {
        return boltConfig;
    }

    public boolean isRelsInBetween() {
        return relsInBetween;
    }
}