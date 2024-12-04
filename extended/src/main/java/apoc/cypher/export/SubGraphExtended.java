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
package apoc.cypher.export;

import apoc.util.CollectionUtilsExtended;
import apoc.util.collection.IterablesExtended;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.TokenRead;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

public interface SubGraphExtended {
    Iterable<Node> getNodes();

    Iterable<Relationship> getRelationships();

    Iterable<IndexDefinition> getIndexes();

    Iterable<ConstraintDefinition> getConstraints();

    Iterable<ConstraintDefinition> getConstraints(Label label);

    Iterable<ConstraintDefinition> getConstraints(RelationshipType type);

    Iterable<IndexDefinition> getIndexes(Label label);

    Iterable<IndexDefinition> getIndexes(RelationshipType label);

    Iterable<RelationshipType> getAllRelationshipTypesInUse();

    Iterable<Label> getAllLabelsInUse();

    Iterator<Node> findNodes(Label label);

    default Map<String, Integer> relTypesInUse(TokenRead ops, Collection<String> relTypeNames) {
        Stream<RelationshipType> stream = IterablesExtended.stream(this.getAllRelationshipTypesInUse());
        if (CollectionUtilsExtended.isNotEmpty(relTypeNames)) {
            stream = stream.filter(rel -> relTypeNames.contains(rel.name()));
        }
        return stream.map(RelationshipType::name).collect(toMap(t -> t, ops::relationshipType));
    }

    default Map<String, Integer> labelsInUse(TokenRead ops, Collection<String> labelNames) {
        Stream<Label> stream = IterablesExtended.stream(this.getAllLabelsInUse());
        if (CollectionUtilsExtended.isNotEmpty(labelNames)) {
            stream = stream.filter(rel -> labelNames.contains(rel.name()));
        }
        return stream.map(Label::name).collect(toMap(t -> t, ops::nodeLabel));
    }

    /**
     * Returns the count for the following pattern (start)-[type]->(end)
     * If start is null will count the following pattern ()-[type]->(end)
     * If end is null will count the following pattern (start)-[type]->()
     * If both start and end are null will count the following pattern ()-[type]->()
     * @param start The start node
     * @param type The relationship type
     * @param end The end node
     * @return the count
     */
    long countsForRelationship(Label start, RelationshipType type, Label end);

    /**
     * Returns the count for the following pattern ()-[type]->(end)
     * @param type The relationship type
     * @param end The end node
     * @return the count
     */
    default long countsForRelationship(RelationshipType type, Label end) {
        return countsForRelationship(null, type, end);
    }

    /**
     * Returns the count for the following pattern (start)-[type]->()
     * @param start The start node
     * @param type The relationship type
     * @return the count
     */
    default long countsForRelationship(Label start, RelationshipType type) {
        return countsForRelationship(start, type, null);
    }

    /**
     * Returns the count for the following pattern ()-[type]->()
     * @param type The relationship type
     * @return the count
     */
    default long countsForRelationship(RelationshipType type) {
        return countsForRelationship(null, type, null);
    }

    long countsForNode(Label label);
}
