package org.neo4j.cypher.export;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;

import java.util.Iterator;

import static apoc.export.cypher.formatter.CypherFormatterUtils.cypherNode;

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
        String startNode = cypherNode(start);
        String endNode = cypherNode(end);
        String relationship = String.format("[r:%s]", type.name());
        return transaction.execute(String.format("MATCH %s-%s->%s RETURN count(r) AS count", startNode, relationship, endNode))
                .<Long>columnAs("count")
                .next();
    }

    @Override
    public long countsForNode(Label label) {
        return transaction.execute(String.format("MATCH (n:%s) RETURN count(n) AS count", label.name()))
                .<Long>columnAs("count")
                .next();
    }

    @Override
    public Iterator<Node> findNodes(Label label) {
        return transaction.findNodes(label);
    }
}
