package apoc.result;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.internal.helpers.collection.MapUtil;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author mh
 * @since 26.02.16
 */
public class VirtualGraph {

    public final Map<String,Object> graph;

    public VirtualGraph(String name, Iterable<Node> nodes, Iterable<Relationship> relationships, Map<String,Object> properties) {
        this.graph = MapUtil.map("name", name,
                "nodes", nodes instanceof Set ? nodes : StreamSupport.stream(nodes.spliterator(), false)
                        .collect(Collectors.toSet()),
                "relationships", relationships instanceof Set ? relationships : StreamSupport.stream(relationships.spliterator(), false)
                        .collect(Collectors.toSet()),
                "properties", properties);
    }

    public Collection<Node> nodes() {
        return (Collection<Node>) this.graph.get("nodes");
    }

    public Collection<Relationship> relationships() {
        return (Collection<Relationship>) this.graph.get("relationships");
    }
}
