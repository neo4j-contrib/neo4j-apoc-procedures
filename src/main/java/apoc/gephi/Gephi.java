package apoc.gephi;

import apoc.Description;
import apoc.graph.Graphs;
import apoc.result.ProgressInfo;
import apoc.util.JsonUtil;
import apoc.util.UrlResolver;
import apoc.util.Util;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.util.MapUtil.map;

/**
 * @author mh
 * @since 29.05.16
 */
// https://github.com/gephi/gephi/wiki/GraphStreaming#Gephi_as_Master
// https://marketplace.gephi.org/plugin/graph-streaming/
public class Gephi {

    private String getGephiUrl(String hostOrKey) {
        return new UrlResolver("http", "localhost", 8080).getUrl("gephi", hostOrKey);
    }


    public static final String[] CAPTIONS = new String[]{"name", "title", "label"};

    // http://127.0.0.1:8080/workspace0?operation=updateGraph
    // TODO configure property-filters or transfer all properties
    @Procedure
    @Description("apoc.gephi.add(url-or-key, workspace, data) | streams passed in data to Gephi")
    public Stream<ProgressInfo> add(@Name("urlOrKey") String keyOrUrl, @Name("workspace") String workspace, @Name("data") Object data) {
        if (workspace == null) workspace = "workspace0";
        String url = getGephiUrl(keyOrUrl)+"/"+Util.encodeUrlComponent(workspace)+"?operation=updateGraph";
        long start = System.currentTimeMillis();
        HashSet<Node> nodes = new HashSet<>(1000);
        HashSet<Relationship> rels = new HashSet<>(10000);
        if (Graphs.extract(data, nodes, rels)) {
            JsonUtil.loadJson(url,map("method","POST"), toGephiStreaming(nodes, "an"));
            JsonUtil.loadJson(url,map("method","POST"), toGephiStreaming(rels,"ae"));
            return Stream.of(new ProgressInfo(url,"graph","gephi").update(nodes.size(),rels.size(),nodes.size()).done(start));
        }
        return Stream.empty();
    }

    private String toGephiStreaming(Collection<? extends PropertyContainer> source, String operation) {
        return source.stream().map(n -> map(operation, info(n))).map(Util::toJson).collect(Collectors.joining("\r\n"));
    }

    private Map<String, Object> info(PropertyContainer pc) {
        if (pc instanceof Node) {
            Node n = (Node) pc;
            return map(idStr(n), map("label", caption(n)));
        }
        if (pc instanceof Relationship) {
            Relationship r = (Relationship) pc;
            return map(String.valueOf(r.getId()), map("label", r.getType().name(), "source",idStr(r.getStartNode()), "target",idStr(r.getEndNode()), "directed",true));
        }
        return map();
    }

    private String idStr(Node n) {
        return String.valueOf(n.getId());
    }

    private String caption(Node n) {
        for (String caption : CAPTIONS) {
            if (n.hasProperty(caption)) return n.getProperty(caption).toString();
        }
        String first=null;
        for (String caption : CAPTIONS) {
            for (String key : n.getPropertyKeys()) {
                if (first==null) first = key;
                if (key.toLowerCase().contains(caption)) return n.getProperty(caption).toString();
            }
        }
        return first == null ? idStr(n) : n.getProperty(first).toString();
    }
}
