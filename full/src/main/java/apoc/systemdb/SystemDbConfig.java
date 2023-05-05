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
package apoc.systemdb;

import apoc.systemdb.metadata.ExportMetadata;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SystemDbConfig {
    public static final String FEATURES_KEY = "features";
    public static final String FILENAME_KEY = "fileName";

    private final List<String> features;
    private final String fileName;

    public SystemDbConfig(Map<String, Object> config) {
        if (config == null) {
            config = Collections.emptyMap();
        }
        List<String> DEFAULT_FEATURES = Stream.of(ExportMetadata.Type.values())
                .map(Enum::name)
                .collect(Collectors.toList());
        this.features = (List<String>) config.getOrDefault(FEATURES_KEY, DEFAULT_FEATURES);
        this.fileName = (String) config.getOrDefault(FILENAME_KEY, "metadata");
    }

    public List<String> getFeatures() {
        return features;
    }

    public String getFileName() {
        return fileName;
    }
}