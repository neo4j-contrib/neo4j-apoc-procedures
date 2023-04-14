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

import apoc.result.NodeResult;
import apoc.result.RelationshipResult;
import apoc.util.Util;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Name;

import java.util.stream.Stream;

public class Get {

    public Transaction tx;

    public Get(Transaction tx) {
        this.tx = tx;
    }

    public Stream<NodeResult> nodes(@Name("nodes") Object ids) {
        return Util.nodeStream(tx, ids).map(NodeResult::new);
    }

    public Stream<RelationshipResult> rels(@Name("relationships") Object ids) {
        return Util.relsStream(tx, ids).map(RelationshipResult::new);
    }

}
