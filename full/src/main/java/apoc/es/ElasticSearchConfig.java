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
package apoc.es;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static apoc.es.ElasticSearchHandler.Version;

public class ElasticSearchConfig {
    public static final String HEADERS_KEY = "headers";
    public static final String VERSION_KEY = "version";

    private final Map<String, Object> headers;
    private final ElasticSearchHandler version;

    public ElasticSearchConfig(Map<String, Object> config) {
        this(config, null);
    }

    public ElasticSearchConfig(Map<String, Object> config, String httpMethod) {
        if (config == null) {
            config = Collections.emptyMap();
        }

        Map<String, Object> headerConf = (Map<String, Object>) config.getOrDefault(HEADERS_KEY, new HashMap<>());
        headerConf.putIfAbsent("content-type", "application/json");
        if (httpMethod != null) {
            headerConf.putIfAbsent("method", httpMethod);
        }
        this.headers = headerConf;
        
        String versionConf = (String) config.getOrDefault(VERSION_KEY, Version.DEFAULT.name());
        this.version = Version.valueOf(versionConf).get();
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public ElasticSearchHandler getVersion() {
        return version;
    }
}
