package org.neo4j.cypher.export;

import org.apache.commons.collections4.CollectionUtils;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.TokenRead;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public interface SubGraph
{
    Iterable<Node> getNodes();

    Iterable<Relationship> getRelationships();

    boolean contains( Relationship relationship );

    Iterable<IndexDefinition> getIndexes();

    Iterable<ConstraintDefinition> getConstraints();

    Iterable<ConstraintDefinition> getConstraints(Label label);

    Iterable<ConstraintDefinition> getConstraints(RelationshipType type);

    Iterable<IndexDefinition> getIndexes(Label label);

    Iterable<IndexDefinition> getIndexes(RelationshipType label);

    Iterable<RelationshipType> getAllRelationshipTypesInUse();

    Iterable<Label> getAllLabelsInUse();

    Iterator<Node> findNodes(Label label);

    default List<RelationshipType> relTypesInUse(Collection<String> relTypeNames) {
        Stream<RelationshipType> stream = Iterables.stream(this.getAllRelationshipTypesInUse());
        if (CollectionUtils.isNotEmpty(relTypeNames)) {
            stream = stream.filter(rel -> relTypeNames.contains(rel.name()));
        }
        return stream.collect(Collectors.toList());
    }

    default List<Label> labelsInUse(Collection<String> labelNames) {
        Stream<Label> stream = Iterables.stream(this.getAllLabelsInUse());
        if (CollectionUtils.isNotEmpty(labelNames)) {
            stream = stream.filter(rel -> labelNames.contains(rel.name()));
        }
        return stream.collect(Collectors.toList());
    }

    /**
     * Returns the count for the following pattern (start)-[type]->(end)
     * If start is null will count the following pattern ()-[type]->(end)
     * If end is null will count the following pattern (start)-[type]->()
     * If both start and end are null will count the following pattern ()-[type]->()
     * @param start The start node
     * @param type The relationship type
     * @param end The end node
     * @return the count
     */
    long countsForRelationship(Label start, RelationshipType type, Label end);

    /**
     * Returns the count for the following pattern ()-[type]->(end)
     * @param type The relationship type
     * @param end The end node
     * @return the count
     */
    default long countsForRelationship(RelationshipType type, Label end) {
        return countsForRelationship(null, type, end);
    }

    /**
     * Returns the count for the following pattern (start)-[type]->()
     * @param start The start node
     * @param type The relationship type
     * @return the count
     */
    default long countsForRelationship(Label start, RelationshipType type) {
        return countsForRelationship(start, type, null);
    }

    /**
     * Returns the count for the following pattern ()-[type]->()
     * @param type The relationship type
     * @return the count
     */
    default long countsForRelationship(RelationshipType type) {
        return countsForRelationship(null, type, null);
    }

    long countsForNode(Label label);

}
