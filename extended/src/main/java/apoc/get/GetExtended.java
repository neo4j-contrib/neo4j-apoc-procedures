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
package apoc.get;

import apoc.result.CreatedNodeResultExtended;
import apoc.result.NodeResultExtended;
import apoc.result.RelationshipResultExtended;
import apoc.result.UpdatedNodeResultExtended;
import apoc.result.UpdatedRelationshipResultExtended;
import apoc.util.UtilExtended;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.procedure.Name;

import java.util.stream.Stream;

public class GetExtended {

    public InternalTransaction tx;

    public GetExtended(InternalTransaction tx) {
        this.tx = tx;
    }

    public Stream<NodeResultExtended> nodes(@Name("nodes") Object ids) {
        return UtilExtended.nodeStream(tx, ids).map(NodeResultExtended::new);
    }

    public Stream<UpdatedNodeResultExtended> updatedNodes(@Name("nodes") Object ids) {
        return UtilExtended.nodeStream(tx, ids).map(UpdatedNodeResultExtended::new);
    }

    public Stream<CreatedNodeResultExtended> createdNodes(@Name("nodes") Object ids) {
        return UtilExtended.nodeStream(tx, ids).map(CreatedNodeResultExtended::new);
    }

    public Stream<RelationshipResultExtended> rels(@Name("rels") Object ids) {
        return UtilExtended.relsStream(tx, ids).map(RelationshipResultExtended::new);
    }

    public Stream<UpdatedRelationshipResultExtended> updatesRels(@Name("rels") Object ids) {
        return UtilExtended.relsStream(tx, ids).map(UpdatedRelationshipResultExtended::new);
    }
}
