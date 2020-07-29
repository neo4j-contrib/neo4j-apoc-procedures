package org.neo4j.cypher.export;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;

public interface SubGraph
{
    Iterable<Node> getNodes();

    Iterable<Relationship> getRelationships();

    boolean contains( Relationship relationship );

    Iterable<IndexDefinition> getIndexes();

    Iterable<ConstraintDefinition> getConstraints();
}
