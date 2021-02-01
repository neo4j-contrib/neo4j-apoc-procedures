package apoc.nodes;

import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DeleteAndReconnectConfig {


    enum RelationshipSelectionStrategy {START, END, MERGE}

    private final RelationshipSelectionStrategy relationshipSelectionStrategy;

    public DeleteAndReconnectConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        this.relationshipSelectionStrategy = RelationshipSelectionStrategy.valueOf(
                (String) config.getOrDefault("relationshipSelectionStrategy", RelationshipSelectionStrategy.START.toString()));
    }

    public RelationshipSelectionStrategy getRelationshipSelectionStrategy() {
        return relationshipSelectionStrategy;
    }
}
