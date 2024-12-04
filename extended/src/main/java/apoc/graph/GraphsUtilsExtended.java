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
package apoc.graph;

import apoc.util.collection.IterablesExtended;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class GraphsUtilsExtended {
    public static boolean extract(Object data, Set<Node> nodes, Set<Relationship> rels) {
        boolean found = false;
        if (data == null) return false;
        if (data instanceof Node) {
            nodes.add((Node) data);
            return true;
        } else if (data instanceof Relationship) {
            rels.add((Relationship) data);
            return true;
        } else if (data instanceof Path) {
            IterablesExtended.addAll(nodes, ((Path) data).nodes());
            IterablesExtended.addAll(rels, ((Path) data).relationships());
            return true;
        } else if (data instanceof Iterable) {
            for (Object o : (Iterable) data) found |= extract(o, nodes, rels);
        } else if (data instanceof Map) {
            for (Object o : ((Map) data).values()) found |= extract(o, nodes, rels);
        } else if (data instanceof Iterator) {
            Iterator it = (Iterator) data;
            while (it.hasNext()) found |= extract(it.next(), nodes, rels);
        } else if (data instanceof Object[]) {
            for (Object o : (Object[]) data) found |= extract(o, nodes, rels);
        }
        return found;
    }
}
