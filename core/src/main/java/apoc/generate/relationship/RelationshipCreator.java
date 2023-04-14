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
package apoc.generate.relationship;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * A component creating {@link Relationship}s with properties.
 */
public interface RelationshipCreator {

    /**
     * Create a relationship between two nodes with properties.
     *
     * @param first  first node.
     * @param second second node.
     * @return created relationship.
     */
    Relationship createRelationship(Node first, Node second);
}
