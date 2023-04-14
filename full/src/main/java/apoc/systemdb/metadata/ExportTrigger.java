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
package apoc.systemdb.metadata;

import apoc.SystemPropertyKeys;
import apoc.export.util.ProgressReporter;
import apoc.util.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.List;
import java.util.Map;

import static apoc.util.Util.toCypherMap;

public class ExportTrigger implements ExportMetadata {

    @Override
    public List<Pair<String, String>> export(Node node, ProgressReporter progressReporter) {
        final String name = (String) node.getProperty(SystemPropertyKeys.name.name());
        final String query = (String) node.getProperty(SystemPropertyKeys.statement.name());
        try {
            final String selector = toCypherMap(JsonUtil.OBJECT_MAPPER.readValue((String) node.getProperty(SystemPropertyKeys.selector.name()), Map.class));
            final String params = toCypherMap(JsonUtil.OBJECT_MAPPER.readValue((String) node.getProperty(SystemPropertyKeys.params.name()), Map.class));
            String statement = String.format("CALL apoc.trigger.add('%s', '%s', %s, {params: %s});", name, query, selector, params);
            if ((boolean) node.getProperty(SystemPropertyKeys.paused.name())) {
                statement += String.format("\nCALL apoc.trigger.pause('%s');", name);
            }
            progressReporter.nextRow();
            return List.of(Pair.of(getFileName(node, Type.Trigger.name()), statement));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}