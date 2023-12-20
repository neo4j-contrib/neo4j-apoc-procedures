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
package apoc.export.arrow;

import static apoc.export.arrow.ArrowUtils.FIELD_ID;
import static apoc.export.arrow.ArrowUtils.FIELD_LABELS;
import static apoc.export.arrow.ArrowUtils.FIELD_SOURCE_ID;
import static apoc.export.arrow.ArrowUtils.FIELD_TARGET_ID;
import static apoc.export.arrow.ArrowUtils.FIELD_TYPE;
import static apoc.export.arrow.ExportArrowStrategy.toField;

import apoc.util.Util;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.internal.helpers.collection.Iterables;

public interface ExportGraphStrategy {

    default Schema schemaFor(GraphDatabaseService db, List<Map<String, Object>> records) {
        final Function<Map<String, Object>, Stream<? extends Field>> flatMapStream = m -> {
            String propertyName = (String) m.get("propertyName");
            List<String> propertyTypes = (List<String>) m.get("propertyTypes");
            return propertyTypes.stream().map(propertyType -> toField(propertyName, new HashSet<>(propertyTypes)));
        };
        final Predicate<Map<String, Object>> filterStream = m -> m.get("propertyName") != null;
        final ResultTransformer<Set<Field>> parsePropertiesResult = result ->
                result.stream().filter(filterStream).flatMap(flatMapStream).collect(Collectors.toSet());

        final Map<String, Object> cfg = records.get(0);
        final Map<String, Object> parameters = Map.of("config", cfg);
        final Set<Field> allFields = new HashSet<>();
        Set<Field> nodeFields = db.executeTransactionally(
                "CALL apoc.meta.nodeTypeProperties($config)", parameters, parsePropertiesResult);

        allFields.add(FIELD_ID);
        allFields.add(FIELD_LABELS);
        allFields.addAll(nodeFields);

        if (cfg.containsKey("includeRels")) {
            final Set<Field> relFields = db.executeTransactionally(
                    "CALL apoc.meta.relTypeProperties($config)", parameters, parsePropertiesResult);
            allFields.add(FIELD_SOURCE_ID);
            allFields.add(FIELD_TARGET_ID);
            allFields.add(FIELD_TYPE);
            allFields.addAll(relFields);
        }
        return new Schema(allFields);
    }

    default Map<String, Object> entityToMap(Entity entity) {
        Map<String, Object> flattened = new HashMap<>();
        flattened.put(FIELD_ID.getName(), entity.getId());
        if (entity instanceof Node) {
            flattened.put(FIELD_LABELS.getName(), Util.labelStrings((Node) entity));
        } else {
            Relationship rel = (Relationship) entity;
            flattened.put(FIELD_TYPE.getName(), rel.getType().name());
            flattened.put(FIELD_SOURCE_ID.getName(), rel.getStartNodeId());
            flattened.put(FIELD_TARGET_ID.getName(), rel.getEndNodeId());
        }
        flattened.putAll(entity.getAllProperties());
        return flattened;
    }

    default Map<String, Object> createConfigMap(SubGraph subGraph, ArrowConfig config) {
        final List<String> allLabelsInUse =
                Iterables.stream(subGraph.getAllLabelsInUse()).map(Label::name).collect(Collectors.toList());
        final List<String> allRelationshipTypesInUse = Iterables.stream(subGraph.getAllRelationshipTypesInUse())
                .map(RelationshipType::name)
                .collect(Collectors.toList());
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("includeLabels", allLabelsInUse);
        if (!allRelationshipTypesInUse.isEmpty()) {
            configMap.put("includeRels", allRelationshipTypesInUse);
        }
        configMap.putAll(config.getConfig());
        return configMap;
    }
}
