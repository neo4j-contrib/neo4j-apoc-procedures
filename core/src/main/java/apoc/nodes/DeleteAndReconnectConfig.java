package apoc.nodes;

import java.util.Collections;
import java.util.Map;

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
