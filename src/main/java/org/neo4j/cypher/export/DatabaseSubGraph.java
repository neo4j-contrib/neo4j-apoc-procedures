package org.neo4j.cypher.export;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;

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
}
