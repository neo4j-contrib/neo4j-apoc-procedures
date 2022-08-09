package apoc.result;

import org.neo4j.graphdb.Node;

import java.util.Collections;
import java.util.Map;

public class NodeValueErrorMapResult {
    public final Node node;
    public final Map<String, Object> value;
    public final Map<String, Object> error;

    public NodeValueErrorMapResult(Node node, Map<String, Object> value, Map<String, Object>  error) {
        this.node = node;
        this.value = value;
        this.error = error;
    }

    public static NodeValueErrorMapResult withError(Node node, Map<String, Object> error) {
        return new NodeValueErrorMapResult(node, Collections.emptyMap(), error);
    }

    public static NodeValueErrorMapResult withResult(Node node, Map<String, Object> value) {
        return new NodeValueErrorMapResult(node, value, Collections.emptyMap());
    }

}
