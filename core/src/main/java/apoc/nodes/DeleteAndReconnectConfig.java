package apoc.nodes;

import apoc.util.Util;
import org.neo4j.graphdb.Relationship;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DeleteAndReconnectConfig {

    private final boolean attachStartRel;
    private final List<Relationship> relsToAttach;
    private final List<String> relTypesToAttach;

    public DeleteAndReconnectConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        this.attachStartRel = Util.toBoolean(config.getOrDefault("attachStartRel", true));
        this.relsToAttach = (List<Relationship>) config.getOrDefault("relsToAttach", Collections.emptyList());
        this.relTypesToAttach = (List<String>) config.getOrDefault("relTypesToAttach", Collections.emptyList());
    }

    public boolean isAttachStartRel() {
        return attachStartRel;
    }

    public List<Relationship> getRelsToAttach() {
        return relsToAttach;
    }

    public List<String> getRelTypesToAttach() {
        return relTypesToAttach;
    }
}
