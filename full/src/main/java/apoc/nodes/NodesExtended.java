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
package apoc.nodes;

import apoc.Extended;
import apoc.util.EntityUtil;
import apoc.util.Util;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

@Extended
public class NodesExtended {

    @Context
    public Transaction tx;
    
    @UserFunction("apoc.node.rebind")
    @Description("apoc.node.rebind(node - to rebind a node (i.e. executing a Transaction.getNodeById(node.getId())  ")
    public Node nodeRebind(@Name("node") Node node) {
        return Util.rebind(tx, node);
    }

    @UserFunction("apoc.rel.rebind")
    @Description("apoc.rel.rebind(rel) - to rebind a rel (i.e. executing a Transaction.getRelationshipById(rel.getId())  ")
    public Relationship relationshipRebind(@Name("rel") Relationship rel) {
        return Util.rebind(tx, rel);
    }

    @UserFunction("apoc.any.rebind")
    @Description("apoc.any.rebind(Object) - to rebind any rel, node, path, map, list or combination of them (i.e. executing a Transaction.getNodeById(node.getId()) / Transaction.getRelationshipById(rel.getId()))")
    public Object anyRebind(@Name("any") Object any) {
        return EntityUtil.anyRebind(tx, any);
    }
}
