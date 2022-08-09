package apoc.generate.relationship;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

/**
 * {@link RelationshipCreator} that creates relationships with the passed in type and no properties.
 */
public class DefaultRelationshipCreator implements RelationshipCreator {

    private final RelationshipType relationshipType;

    public DefaultRelationshipCreator(String relationshipType) {
        this.relationshipType = RelationshipType.withName(relationshipType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Relationship createRelationship(Node first, Node second) {
        return first.createRelationshipTo(second, relationshipType);
    }
}
