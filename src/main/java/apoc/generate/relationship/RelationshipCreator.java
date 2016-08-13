package apoc.generate.relationship;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.unsafe.batchinsert.BatchInserter;

/**
 * A component creating {@link Relationship}s with properties.
 */
public interface RelationshipCreator {

    /**
     * Create a relationship between two nodes with properties.
     *
     * @param first  first node.
     * @param second second node.
     * @return created relationship.
     */
    Relationship createRelationship(Node first, Node second);
}
