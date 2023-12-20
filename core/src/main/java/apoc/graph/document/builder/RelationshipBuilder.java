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
package apoc.graph.document.builder;

import apoc.graph.util.GraphsConfig;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class RelationshipBuilder {

    private GraphsConfig config;

    public RelationshipBuilder(GraphsConfig config) {
        this.config = config;
    }

    public Collection<Relationship> buildRelation(Node parent, Node child, String relationName) {

        RelationshipType type = RelationshipType.withName(
                config.getRelMapping().getOrDefault(relationName, relationName.toUpperCase()));

        // check if already exists
        // find only relation between parent and child node
        List<Relationship> rels = getRelationshipsForRealNodes(parent, child, type);
        if (rels.isEmpty()) {
            return Collections.singleton(parent.createRelationshipTo(child, type));
        } else {
            return rels;
        }
    }

    private List<Relationship> getRelationshipsForRealNodes(Node parent, Node child, RelationshipType type) {
        Iterable<Relationship> relationships = child.getRelationships(Direction.INCOMING, type);
        return StreamSupport.stream(relationships.spliterator(), false)
                .filter(rel -> rel.getOtherNode(child).equals(parent))
                .collect(Collectors.toList());
    }
}
