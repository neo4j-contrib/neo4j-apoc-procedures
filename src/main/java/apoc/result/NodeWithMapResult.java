package apoc.result;

import org.neo4j.graphdb.Node;

import java.util.Map;

/**
 * @author mh
 * @since 26.02.16
 */
public class NodeWithMapResult {
    public final Node node;
    public final Map<String, Object> value;

    public NodeWithMapResult(Node node, Map<String, Object> value) {
        this.node = node;
        this.value = value;
    }
}
