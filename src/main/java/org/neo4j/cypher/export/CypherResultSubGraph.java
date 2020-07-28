package org.neo4j.cypher.export;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.helpers.collection.Iterables;

import java.util.*;

import static org.neo4j.internal.helpers.collection.Iterators.loop;

public class CypherResultSubGraph implements SubGraph
{

    private final SortedMap<Long, Node> nodes = new TreeMap<>();
    private final SortedMap<Long, Relationship> relationships = new TreeMap<>();
    private final Collection<Label> labels = new HashSet<>();
    private final Collection<IndexDefinition> indexes = new HashSet<>();
    private final Collection<ConstraintDefinition> constraints = new HashSet<>();

    public void add( Node node )
    {
        final long id = node.getId();
        if ( !nodes.containsKey( id ) )
        {
            addNode( id, node );
        }
    }

    void addNode( long id, Node data )
    {
        nodes.put( id, data );
        labels.addAll( Iterables.asCollection( data.getLabels() ) );
    }

    public void add( Relationship rel )
    {
        final long id = rel.getId();
        if ( !relationships.containsKey( id ) )
        {
            addRel( id, rel );
            add( rel.getStartNode() );
            add( rel.getEndNode() );
        }
    }

    public static SubGraph from(Transaction tx, Result result, boolean addBetween)
    {
        final CypherResultSubGraph graph = new CypherResultSubGraph();
        final List<String> columns = result.columns();
        for ( Map<String, Object> row : loop( result ) )
        {
            for ( String column : columns )
            {
                final Object value = row.get( column );
                graph.addToGraph( value );
            }
        }
        for ( IndexDefinition def : tx.schema().getIndexes() )
        {
            for ( Label label : def.getLabels() )
            {
                if ( graph.getLabels().contains( label ) )
                {
                    graph.addIndex( def );
                    break;
                }
            }
        }
        for ( ConstraintDefinition def : tx.schema().getConstraints() )
        {
            if ( graph.getLabels().contains( def.getLabel() ) )
            {
                graph.addConstraint( def );
            }
        }
        if ( addBetween )
        {
            graph.addRelationshipsBetweenNodes();
        }
        return graph;
    }

    private void addIndex( IndexDefinition def )
    {
        indexes.add( def );
    }

    private void addConstraint( ConstraintDefinition def )
    {
        constraints.add( def );
    }

    private void addRelationshipsBetweenNodes()
    {
        Set<Node> newNodes = new HashSet<>();
        for ( Node node : nodes.values() )
        {
            for ( Relationship relationship : node.getRelationships() )
            {
                if ( !relationships.containsKey( relationship.getId() ) )
                {
                    continue;
                }

                final Node other = relationship.getOtherNode( node );
                if ( nodes.containsKey( other.getId() ) || newNodes.contains( other ) )
                {
                    continue;
                }
                newNodes.add( other );
            }
        }
        for ( Node node : newNodes )
        {
            add( node );
        }
    }

    private void addToGraph( Object value )
    {
        if ( value instanceof Node )
        {
            add( (Node) value );
        }
        if ( value instanceof Relationship )
        {
            add( (Relationship) value );
        }
        if ( value instanceof Iterable )
        {
            for ( Object inner : (Iterable) value )
            {
                addToGraph( inner );
            }
        }
    }

    @Override
    public Iterable<Node> getNodes()
    {
        return nodes.values();
    }

    @Override
    public Iterable<Relationship> getRelationships()
    {
        return relationships.values();
    }

    public Collection<Label> getLabels()
    {
        return labels;
    }

    void addRel( Long id, Relationship rel )
    {
        relationships.put( id, rel );
    }

    @Override
    public boolean contains( Relationship relationship )
    {
        return relationships.containsKey( relationship.getId() );
    }

    @Override
    public Iterable<IndexDefinition> getIndexes()
    {
        return indexes;
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints()
    {
        return constraints;
    }

}
