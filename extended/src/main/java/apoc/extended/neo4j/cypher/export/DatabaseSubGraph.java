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
package apoc.extended.neo4j.cypher.export;

import static apoc.export.cypher.formatter.CypherFormatterUtils.cypherNode;
import static apoc.extended.util.Util.quoteIdentifierSafely;

import apoc.extended.util.Util;
import java.util.Comparator;
import java.util.Iterator;
import java.util.stream.StreamSupport;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.token.api.TokenConstants;

public class DatabaseSubGraph implements SubGraph {
    private final Transaction transaction;

    public DatabaseSubGraph(Transaction transaction) {
        this.transaction = transaction;
    }

    public static SubGraph optimizedForCount(Transaction transaction, KernelTransaction kernelTransaction) {
        return new CountOptimisedDatabaseSubGraph(transaction, kernelTransaction);
    }

    @Override
    public ResourceIterable<Node> getNodes() {
        return transaction.getAllNodes();
    }

    @Override
    public ResourceIterable<Relationship> getRelationships() {
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
        String relationship = String.format("[r:%s]", quoteIdentifierSafely(type.name()));
        try (Result result = transaction.execute(
                String.format("MATCH %s-%s->%s RETURN count(r) AS count", startNode, relationship, endNode))) {
            return result.<Long>columnAs("count").next();
        }
    }

    @Override
    public long countsForNode(Label label) {
        try (Result result = transaction.execute(
                String.format("MATCH (n:%s) RETURN count(n) AS count", quoteIdentifierSafely(label.name())))) {
            return result.<Long>columnAs("count").next();
        }
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
class CountOptimisedDatabaseSubGraph extends DatabaseSubGraph {
    private final TokenRead tokenRead;
    private final Read read;

    public CountOptimisedDatabaseSubGraph(Transaction transaction, KernelTransaction kernelTx) {
        super(transaction);
        this.tokenRead = kernelTx.tokenRead();
        this.read = kernelTx.dataRead();
    }

    @Override
    public long countsForNode(Label label) {
        int nodeLabelID = tokenRead.nodeLabel(label.name());
        if (nodeLabelID < 0) {
            return 0;
        }
        return read.countsForNode(nodeLabelID);
    }

    @Override
    public long countsForRelationship(RelationshipType type, Label end) {
        int relTypeID = tokenRead.relationshipType(type.name());
        int nodeLabelID = tokenRead.nodeLabel(end.name());
        if (nodeLabelID < 0 || relTypeID < 0) {
            return 0;
        }
        return read.countsForRelationship(TokenConstants.ANY_LABEL, relTypeID, nodeLabelID);
    }

    @Override
    public long countsForRelationship(Label start, RelationshipType type) {
        int relTypeID = tokenRead.relationshipType(type.name());
        int nodeLabelID = tokenRead.nodeLabel(start.name());
        if (nodeLabelID < 0 || relTypeID < 0) {
            return 0;
        }
        return read.countsForRelationship(nodeLabelID, relTypeID, TokenConstants.ANY_LABEL);
    }

    @Override
    public long countsForRelationship(RelationshipType type) {
        int relTypeID = tokenRead.relationshipType(type.name());
        if (relTypeID < 0) {
            return 0;
        }
        return read.countsForRelationship(TokenConstants.ANY_LABEL, relTypeID, TokenConstants.ANY_LABEL);
    }
}
