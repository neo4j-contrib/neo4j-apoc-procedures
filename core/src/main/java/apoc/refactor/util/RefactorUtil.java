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
package apoc.refactor.util;

import org.neo4j.graphdb.*;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static apoc.util.Util.isSelfRel;

public class RefactorUtil {

    public static void mergeRelsWithSameTypeAndDirectionInMergeNodes(Node node, RefactorConfig config, Direction dir, List<Long> excludeRelIds) {
        for (RelationshipType type : node.getRelationshipTypes()) {
            StreamSupport.stream(node.getRelationships(dir,type).spliterator(), false)
                    .filter(rel -> !excludeRelIds.contains(rel.getId()))
                    .collect(Collectors.groupingBy(rel -> Pair.of(rel.getStartNode(), rel.getEndNode())))
                    .values().stream()
                    .filter(list -> !list.isEmpty())
                    .forEach(list -> {
                        Relationship first = list.get(0);
                        if (isSelfRel(first) && !config.isCreatingNewSelfRel()) {
                            list.forEach(Relationship::delete);
                        } else {
                            for (int i = 1; i < list.size(); i++) {
                                Relationship relationship = list.get(i);
                                mergeRels(relationship, first, true,  config);
                            }
                        }
                    });
        }
    }

    public static void mergeRels(Relationship source, Relationship target, boolean delete, RefactorConfig conf) {
        Map<String, Object> properties = source.getAllProperties();
        if (delete) {
            source.delete();
        }
        PropertiesManager.mergeProperties(properties, target, conf);
    }

    public static <T extends Entity> T copyProperties(Entity source, T target) {
        return copyProperties(source.getAllProperties(), target);
    }

    public static <T extends Entity> T copyProperties(Map<String, Object> source, T target) {
        for (Map.Entry<String, Object> prop : source.entrySet()) {
            target.setProperty(prop.getKey(), prop.getValue());
        }
        return target;
    }

}
