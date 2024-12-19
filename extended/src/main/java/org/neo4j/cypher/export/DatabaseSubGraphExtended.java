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
package org.neo4j.cypher.export;

import apoc.util.Util;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.token.api.TokenConstants;

import java.util.Comparator;
import java.util.Iterator;
import java.util.stream.StreamSupport;

import static apoc.export.cypher.formatter.CypherFormatterUtils.cypherNode;
import static apoc.util.Util.quote;

public class DatabaseSubGraphExtended implements SubGraph {
    private final Transaction transaction;

    public DatabaseSubGraphExtended(Transaction transaction) {
        this.transaction = transaction;
    }

    public static SubGraph optimizedForCount(Transaction transaction, KernelTransaction kernelTransaction) {
        return new CountOptimisedDatabaseSubGraphExtended(transaction, kernelTransaction);
    }

    @Override
    public Iterable<Node> getNodes() {
        return transaction.getAllNodes();
    }

    @Override
    public Iterable<Relationship> getRelationships() {
        return transaction.getAllRelationships();
    }

    @Override
    public Iterable<IndexDefinition> getIndexes() {
        return Util.getIndexes(transaction);
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints() {
        Comparator<ConstraintDefinition> comp = Comparator.comparing(ConstraintDefinition::getName);
        return StreamSupport.stream(transaction.schema().getConstraints().spliterator(), false)
                .sorted(comp)
                .toList();
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints(Label label) {
        return transaction.schema().getConstraints(label);
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints(RelationshipType type) {
        return transaction.schema().getConstraints(type);
    }

    @Override
    public Iterable<IndexDefinition> getIndexes(Label label) {
        return Util.getIndexes(transaction, label);
    }

    @Override
    public Iterable<IndexDefinition> getIndexes(RelationshipType type) {
        return Util.getIndexes(transaction, type);
    }

    @Override
    public Iterable<RelationshipType> getAllRelationshipTypesInUse() {
        return transaction.getAllRelationshipTypesInUse();
    }

    @Override
    public Iterable<Label> getAllLabelsInUse() {
        return transaction.getAllLabelsInUse();
    }

    @Override
    public long countsForRelationship(Label start, RelationshipType type, Label end) {
        String startNode = cypherNode(start);
        String endNode = cypherNode(end);
        String relationship = String.format("[r:%s]", quote(type.name()));
        return transaction
                .execute(String.format("MATCH %s-%s->%s RETURN count(r) AS count", startNode, relationship, endNode))
                .<Long>columnAs("count")
                .next();
    }

    @Override
    public long countsForNode(Label label) {
        return transaction
                .execute(String.format("MATCH (n:%s) RETURN count(n) AS count", quote(label.name())))
                .<Long>columnAs("count")
                .next();
    }

    @Override
    public Iterator<Node> findNodes(Label label) {
        return transaction.findNodes(label);
    }
}

/**
 * Implementation of DatabaseSubGraph that uses internal kernel APIs directly for better performance when retrieving counts.
 * The default implementation can cause a lot of subqueries that requires time for planning.
 */
class CountOptimisedDatabaseSubGraphExtended extends DatabaseSubGraphExtended {
    private final TokenRead tokenRead;
    private final Read read;

    public CountOptimisedDatabaseSubGraphExtended(Transaction transaction, KernelTransaction kernelTx) {
        super(transaction);
        this.tokenRead = kernelTx.tokenRead();
        this.read = kernelTx.dataRead();
    }

    @Override
    public long countsForNode(Label label) {
        return read.countsForNode(tokenRead.nodeLabel(label.name()));
    }

    @Override
    public long countsForRelationship(RelationshipType type, Label end) {
        return read.countsForRelationship(
                TokenConstants.ANY_LABEL, tokenRead.relationshipType(type.name()), tokenRead.nodeLabel(end.name()));
    }

    @Override
    public long countsForRelationship(Label start, RelationshipType type) {
        return read.countsForRelationship(
                tokenRead.nodeLabel(start.name()), tokenRead.relationshipType(type.name()), TokenConstants.ANY_LABEL);
    }

    @Override
    public long countsForRelationship(RelationshipType type) {
        return read.countsForRelationship(
                TokenConstants.ANY_LABEL, tokenRead.relationshipType(type.name()), TokenConstants.ANY_LABEL);
    }
}
