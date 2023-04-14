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
package apoc.lock;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.*;

import java.util.List;

public class Lock {

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.lock.all([nodes],[relationships]) acquires a write lock on the given nodes and relationships")
    public void all(@Name("nodes") List<Node> nodes, @Name("rels") List<Relationship> rels) {
        for (Node node : nodes) {
            tx.acquireWriteLock(node);
        }
        for (Relationship rel : rels) {
            tx.acquireWriteLock(rel);
        }
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.lock.nodes([nodes]) acquires a write lock on the given nodes")
    public void nodes(@Name("nodes") List<Node> nodes) {
        for (Node node : nodes) {
            tx.acquireWriteLock(node);
        }
    }

    @Procedure(mode = Mode.READ, name = "apoc.lock.read.nodes")
    @Description("apoc.lock.read.nodes([nodes]) acquires a read lock on the given nodes")
    public void readLockOnNodes(@Name("nodes") List<Node> nodes) {
        for (Node node : nodes) {
            tx.acquireReadLock(node);
        }
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.lock.rels([relationships]) acquires a write lock on the given relationship")
    public void rels(@Name("rels") List<Relationship> rels) {
        for (Relationship rel : rels) {
            tx.acquireWriteLock(rel);
        }
    }

    @Procedure(mode = Mode.READ, name = "apoc.lock.read.rels")
    @Description("apoc.lock.read.rels([relationships]) acquires a read lock on the given relationship")
    public void readLocksOnRels(@Name("rels") List<Relationship> rels) {
        for (Relationship rel : rels) {
            tx.acquireReadLock(rel);
        }
    }
}
