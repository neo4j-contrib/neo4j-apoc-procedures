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
package apoc.algo;

import apoc.result.RelationshipResult;
import apoc.util.Util;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Cover {

    @Context
    public Transaction tx;

    @Procedure
    @Description("apoc.algo.cover(nodes) yield rel - returns all relationships between this set of nodes")
    public Stream<RelationshipResult> cover(@Name("nodes") Object nodes) {
        Set<Node> nodeSet = Util.nodeStream(tx, nodes).collect(Collectors.toSet());
        return coverNodes(nodeSet).map(RelationshipResult::new);
    }

    // non-parallelized utility method for use by other procedures
    public static Stream<Relationship> coverNodes(Collection<Node> nodes) {
        Set<Node> nodeSet = new HashSet<>(nodes);
        return nodes.stream()
                .flatMap(n ->
                        StreamSupport.stream(n.getRelationships(Direction.OUTGOING)
                                .spliterator(),false)
                                .filter(r -> nodeSet.contains(r.getEndNode())));
    }
}
