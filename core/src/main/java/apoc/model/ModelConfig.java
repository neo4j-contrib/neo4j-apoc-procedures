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
package apoc.model;

import apoc.util.Util;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ModelConfig {

    private final boolean write;

    private final String schema;

    private final List<String> tables;
    private final List<String> views;
    private final List<String> columns;

    public ModelConfig(Map<String, Object> config) {
        this.write = Util.toBoolean(config.getOrDefault("write",false));
        Map<String, List<String>> filters = (Map<String, List<String>>) config.getOrDefault("filters", Collections.emptyMap());
        this.tables = toPatternList(filters.getOrDefault("tables", Collections.emptyList()));
        this.views = toPatternList(filters.getOrDefault("views", Collections.emptyList()));
        this.columns = toPatternList(filters.getOrDefault("columns", Collections.emptyList()));
        this.schema = config.getOrDefault("schema", "").toString();
    }


    private List<String> toPatternList(List<String> patterns) {
        return patterns
                .stream()
                .collect(Collectors.toList());
    }

    public List<String> getViews() {
        return views;
    }

    public List<String> getColumns() {
        return columns;
    }

    public boolean isWrite() {
        return write;
    }

    public List<String> getTables() {
        return tables;
    }

    public String getSchema() {
        return schema;
    }
}
