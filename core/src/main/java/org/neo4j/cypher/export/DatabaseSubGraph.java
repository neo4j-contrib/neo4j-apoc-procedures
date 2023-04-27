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

import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;

import java.util.Iterator;
import java.util.Optional;

import static apoc.export.cypher.formatter.CypherFormatterUtils.cypherNode;
import static org.neo4j.internal.kernel.api.TokenRead.ANY_LABEL;
import static apoc.util.Util.quote;

public class DatabaseSubGraph implements SubGraph
{
    private final Transaction transaction;

    public DatabaseSubGraph( Transaction transaction )
    {
        this.transaction = transaction;
    }

    public static SubGraph from( Transaction transaction )
    {
        return new DatabaseSubGraph( transaction );
    }

    @Override
    public Iterable<Node> getNodes()
    {
        return transaction.getAllNodes();
    }

    @Override
    public Iterable<Relationship> getRelationships()
    {
        return transaction.getAllRelationships();
    }

    @Override
    public boolean contains( Relationship relationship )
    {
        return transaction.getRelationshipById( relationship.getId() ) != null;
    }

    @Override
    public Iterable<IndexDefinition> getIndexes()
    {
        return transaction.schema().getIndexes();
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints()
    {
        Comparator<ConstraintDefinition> comp = Comparator.comparing(ConstraintDefinition::getName);
        return StreamSupport.stream( transaction.schema().getConstraints().spliterator(), false ).sorted(comp).collect( Collectors.toList());
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
        return transaction.schema().getIndexes(label);
    }

    @Override
    public Iterable<IndexDefinition> getIndexes(RelationshipType type) {
        return transaction.schema().getIndexes(type);
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
        // even if `MATCH ()-[r]->(:Label) RETURN count(r)` and `MATCH (:Label)-[r]->() RETURN count(r)` leverage on `RelationshipCountFromCountStore`
        // given that with this method there is planner overhead, we count entities via the `TokenRead`, because it's much faster 
        // and we fallback to query via count store if transaction is not an InternalTransaction
        if (transaction instanceof InternalTransaction) {
            final KernelTransaction kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            final TokenRead tokenRead = kernelTransaction.tokenRead();
            final int startId = getLabelId(start, tokenRead);
            final int endId = getLabelId(end, tokenRead);
            if ( labelNotExists(start, startId) || labelNotExists(end, endId) ) {
                return 0L;
            }
            final int relId = tokenRead.relationshipType(type.name());

            return kernelTransaction.dataRead()
                    .countsForRelationship(startId, relId, endId);
        }
        String startNode = cypherNode(start);
        String endNode = cypherNode(end);
        String relationship = String.format("[r:%s]", quote(type.name()));
        return transaction.execute(String.format("MATCH %s-%s->%s RETURN count(r) AS count", startNode, relationship, endNode))
                .<Long>columnAs("count")
                .next();
    }

    @Override
    public long countsForNode(Label label) {
        // even if `MATCH (n:Label) RETURN count(n)` leverage on `NodeCountFromCountStore`
        // given that with this method there is planner overhead, we count entities via the `TokenRead`, because it's much faster 
        // and we fallback to query via count store if transaction is not an InternalTransaction
        if (transaction instanceof InternalTransaction) {
            final KernelTransaction kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            final int labelId = getLabelId(label, kernelTransaction.tokenRead());
            if (labelNotExists(label, labelId)) {
                return 0L;
            }
            return kernelTransaction.dataRead()
                    .countsForNode(labelId);
        }
        return transaction.execute(String.format("MATCH (n:%s) RETURN count(n) AS count", quote(label.name())))
                .<Long>columnAs("count")
                .next();
    }

    private boolean labelNotExists(Label label, int labelId) {
        // `label == null` means that we want to count all the labels
        // otherwise if we don't found a not-null label, we want the count to return to zero
        return label != null && labelId == ANY_LABEL;
    }

    private Integer getLabelId(Label start, TokenRead tokenRead) {
        return Optional.ofNullable(start)
                .map(Label::name)
                .map(tokenRead::nodeLabel)
                .orElse(ANY_LABEL);
    }

    @Override
    public Iterator<Node> findNodes(Label label) {
        return transaction.findNodes(label);
    }
}
