package apoc.nodes;

import org.neo4j.graphdb.Relationship;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DeleteAndReconnectConfig {

    enum RelationshipSelectionStrategy {START, END, MERGE}

    private final RelationshipSelectionStrategy relationshipSelectionStrategy;
    private final List<Relationship> relsToAttach;
    private final List<String> relTypesToAttach;

    public DeleteAndReconnectConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        this.relationshipSelectionStrategy = RelationshipSelectionStrategy.valueOf((String) config.getOrDefault("relationshipSelectionStrategy", "START"));
        this.relsToAttach = (List<Relationship>) config.getOrDefault("relsToAttach", Collections.emptyList());
        this.relTypesToAttach = (List<String>) config.getOrDefault("relTypesToAttach", Collections.emptyList());
    }

    public RelationshipSelectionStrategy getRelationshipSelectionStrategy() {
        return relationshipSelectionStrategy;
    }

    public List<Relationship> getRelsToAttach() {
        return relsToAttach;
    }

    public List<String> getRelTypesToAttach() {
        return relTypesToAttach;
    }
}
