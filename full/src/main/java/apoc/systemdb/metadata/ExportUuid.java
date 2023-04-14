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
import org.neo4j.graphdb.Node;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static apoc.util.Util.toCypherMap;

public class ExportUuid implements ExportMetadata {

    @Override
    public List<Pair<String, String>> export(Node node, ProgressReporter progressReporter) {
        Map<String, Object> map = new HashMap<>();
        final String labelName = (String) node.getProperty(SystemPropertyKeys.label.name());
        final String property = (String) node.getProperty(SystemPropertyKeys.propertyName.name());
        map.put("addToSetLabels", node.getProperty(SystemPropertyKeys.addToSetLabel.name(), null));
        map.put("uuidProperty", property);
        final String uuidConfig = toCypherMap(map);
        // add constraint - TODO: might be worth add config to export or not this file
        String schemaStatement = String.format("CREATE CONSTRAINT IF NOT EXISTS ON (n:%s) ASSERT n.%s IS UNIQUE;\n", labelName, property);
        final String statement = String.format("CALL apoc.uuid.install('%s', %s);", labelName, uuidConfig);
        progressReporter.nextRow();
        return List.of(
                Pair.of(getFileName(node, Type.Uuid.name() + ".schema"), schemaStatement),
                Pair.of(getFileName(node, Type.Uuid.name()), statement)
        );

    }
}
