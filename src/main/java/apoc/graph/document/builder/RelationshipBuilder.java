package apoc.graph.document.builder;

import apoc.graph.util.GraphsConfig;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class RelationshipBuilder {

    private GraphsConfig config;

    public RelationshipBuilder(GraphsConfig config) {
        this.config = config;
    }

    public Collection<Relationship> buildRelation(Node parent, Node child, String relationName) {
        RelationshipType type = RelationshipType.withName(relationName.toUpperCase());

        // check if already exists
        // find only relation between parent and child node
        List<Relationship> rels = getRelationshipsForRealNodes(parent, child, type);
        if (rels.isEmpty()) {
            return Collections.singleton(parent.createRelationshipTo(child, type));
        } else {
            return rels;
        }
    }

    private List<Relationship> getRelationshipsForRealNodes(Node parent, Node child, RelationshipType type) {
        Iterable<Relationship> relationships = child.getRelationships(Direction.INCOMING, type);
        return StreamSupport.stream(relationships.spliterator(), false)
                .filter(rel -> rel.getOtherNode(child).equals(parent))
                .collect(Collectors.toList());
    }

}