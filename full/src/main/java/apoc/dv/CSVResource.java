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
package apoc.dv;

import java.util.HashMap;
import java.util.Map;

public class CSVResource extends VirtualizedResource {

    public CSVResource(String name, Map<String, Object> config) {
        super(name, config, "CSV");
    }

    @Override
    protected Map<String, Object> getProcedureParameters(Object queryParams, Map<String, Object> config) {
        final Map<String, Object> base = Map.of("url", this.url, "config", config, "labels", labels);
        Map<String, Object> map = new HashMap<>();
        map.putAll((Map<String, Object>) queryParams);
        map.putAll(base);
        return map;
    }

    @Override
    protected String getProcedureCall(Map<String, Object> config) {
        return "CALL apoc.load.csv($url, $config) YIELD map WHERE " + query
                + " RETURN apoc.create.vNode($labels, map) AS node";
    }

}
