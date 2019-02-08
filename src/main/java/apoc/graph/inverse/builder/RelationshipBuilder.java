package apoc.graph.inverse.builder;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class RelationshipBuilder {

    public void buildRelation(Node parent, Node child) {
        String relationName = child.getProperty("type").toString().toUpperCase();

        RelationshipType type = RelationshipType.withName(relationName);

        //check if already exists
        Iterable<Relationship> relationships = child.getRelationships(Direction.INCOMING, type);

        //find only relation between parent and child node
        List<Relationship> rels = StreamSupport.stream(relationships.spliterator(), false)
                .filter(rel -> rel.getStartNode().getId() == parent.getId())
                .collect(Collectors.toList());

        if (rels.isEmpty()) {
            parent.createRelationshipTo(child, type);
        }
    }

}