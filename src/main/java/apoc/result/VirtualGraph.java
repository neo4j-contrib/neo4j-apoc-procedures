package apoc.result;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.MapUtil;

import java.util.List;
import java.util.Map;

/**
 * @author mh
 * @since 26.02.16
 */
public class VirtualGraph {

    public final Map<String,Object> graph;

    public VirtualGraph(String name, Iterable<Node> nodes, Iterable<Relationship> relationships, Map<String,Object> properties) {
        this.graph = MapUtil.map("name",name,"nodes",nodes,"relationships",relationships,"properties",properties);
    }
}
