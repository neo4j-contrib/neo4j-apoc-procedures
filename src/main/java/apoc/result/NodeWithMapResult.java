package apoc.result;

import org.neo4j.graphdb.Node;

import java.util.Collections;
import java.util.Map;

public class NodeWithMapResult {
    public final Node node;
    public final Map<String, Object> value;
    public final Map<String, Object> error;

    public NodeWithMapResult(Node node, Map<String, Object> value, Map<String, Object>  error) {
        this.node = node;
        this.value = value;
        this.error = error;
    }

    public static NodeWithMapResult withError(Node node, Map<String, Object> error) {
        return new NodeWithMapResult(node, Collections.emptyMap(), error);
    }

    public static NodeWithMapResult withResult(Node node, Map<String, Object> value) {
        return new NodeWithMapResult(node, value, Collections.emptyMap());
    }

}
