package org.neo4j.cypher.export;

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
        return transaction.schema().getConstraints();
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
            final int relId = tokenRead.relationshipType(type.name());
            final int endId = getLabelId(end, tokenRead);

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
            return kernelTransaction.dataRead()
                    .countsForNode(labelId);
        }
        return transaction.execute(String.format("MATCH (n:%s) RETURN count(n) AS count", quote(label.name())))
                .<Long>columnAs("count")
                .next();
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
