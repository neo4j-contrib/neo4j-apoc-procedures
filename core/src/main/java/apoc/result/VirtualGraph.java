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
package apoc.result;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.internal.helpers.collection.MapUtil;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author mh
 * @since 26.02.16
 */
public class VirtualGraph {

    public final Map<String,Object> graph;

    public VirtualGraph(String name, Iterable<Node> nodes, Iterable<Relationship> relationships, Map<String,Object> properties) {
        this.graph = MapUtil.map("name", name,
                "nodes", nodes instanceof Set ? nodes : StreamSupport.stream(nodes.spliterator(), false)
                        .collect(Collectors.toSet()),
                "relationships", relationships instanceof Set ? relationships : StreamSupport.stream(relationships.spliterator(), false)
                        .collect(Collectors.toSet()),
                "properties", properties);
    }

    public Collection<Node> nodes() {
        return (Collection<Node>) this.graph.get("nodes");
    }

    public Collection<Relationship> relationships() {
        return (Collection<Relationship>) this.graph.get("relationships");
    }
}
