package apoc.gephi;

import apoc.Extended;
import apoc.graph.Graphs;
import apoc.result.ProgressInfo;
import apoc.util.JsonUtil;
import apoc.util.UrlResolver;
import apoc.util.Util;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.util.MapUtil.map;

/**
 * @author mh
 * @since 29.05.16
 */
// https://github.com/gephi/gephi/wiki/GraphStreaming#Gephi_as_Master
// https://marketplace.gephi.org/plugin/graph-streaming/
@Extended
public class Gephi {

    public static final int WIDTH = 1000;
    public static final int HEIGHT = 1000;

    private String getGephiUrl(String hostOrKey) {
        return new UrlResolver("http", "localhost", 8080).getUrl("gephi", hostOrKey);
    }

    public static double doubleValue(Entity pc, String prop, Number defaultValue) {
       return Util.toDouble(pc.getProperty(prop, defaultValue));

    }

    private static final String[] CAPTIONS = new String[]{"name", "title", "label"};
    private static final List<String> RESERVED = Arrays.asList("label", "TYPE", "id", "source", "target", "weight", "directed");
    // http://127.0.0.1:8080/workspace0?operation=updateGraph
    // TODO configure property-filters or transfer all properties
    @Procedure
    @Description("apoc.gephi.add(url-or-key, workspace, data, weightproperty, ['exportproperty']) | streams passed in data to Gephi")
    public Stream<ProgressInfo> add(@Name("urlOrKey") String keyOrUrl, @Name("workspace") String workspace, @Name("data") Object data,@Name(value = "weightproperty",defaultValue = "null") String weightproperty,@Name(value = "exportproperties",defaultValue = "[]") List<String> exportproperties) {
        if (workspace == null) workspace = "workspace0";
        String url = getGephiUrl(keyOrUrl)+"/"+Util.encodeUrlComponent(workspace)+"?operation=updateGraph";
        long start = System.currentTimeMillis();
        HashSet<Node> nodes = new HashSet<>(1000);
        HashSet<Relationship> rels = new HashSet<>(10000);
        List<String> propertyNames = new ArrayList<>(exportproperties);
        propertyNames.removeAll(RESERVED);
        if (Graphs.extract(data, nodes, rels)) {
            String payload = toGephiStreaming(nodes, rels, weightproperty, propertyNames.toArray(new String[propertyNames.size()]));
            JsonUtil.loadJson(url,map("method","POST","Content-Type","application/json; charset=utf-8"), payload).count();
            return Stream.of(new ProgressInfo(url,"graph","gephi").update(nodes.size(),rels.size(),nodes.size()).done(start));
        }
        return Stream.empty();
    }

    private String toGephiStreaming(Collection<Node> nodes, Collection<Relationship> rels,String weightproperty, String[] exportproperties) {
        return Stream.concat(toGraphStream(nodes, "an", weightproperty, exportproperties), toGraphStream(rels, "ae", weightproperty, exportproperties)).collect(Collectors.joining("\r\n"));
    }

    private Stream<String> toGraphStream(Collection<? extends Entity> source, String operation,String weightproperty, String[] exportproperties) {
        Map<String,Map<String,Object>> colors=new HashMap<>();
        return source.stream().map(n -> map(operation, data(n,colors,weightproperty, exportproperties))).map(Util::toJson);
    }

    private Map<String, Object> data(Entity pc, Map<String, Map<String, Object>> colors, String weightproperty, String[] exportproperties) {
        if (pc instanceof Node) {
            Node n = (Node) pc;
            String labels = Util.labelString(n);
            Map<String, Object> attributes = map("label", caption(n), "TYPE", labels);
            attributes.putAll(positions());
            attributes.putAll(color(labels,colors));
            if (exportproperties.length > 0) attributes.putAll(n.getProperties(exportproperties));
            return map(idStr(n), attributes);
        }
        if (pc instanceof Relationship) {
            Relationship r = (Relationship) pc;
            String type = r.getType().name();
            Map<String, Object> attributes = map("label", type, "TYPE", type);
            Double weight = Util.doubleValue(r,weightproperty,1.0);
            attributes.putAll(map("source", idStr(r.getStartNode()), "target", idStr(r.getEndNode()), "directed", true,"weight",weight));
            attributes.putAll(color(type, colors));
            if (exportproperties.length > 0) attributes.putAll(r.getProperties(exportproperties));
            return map(String.valueOf(r.getId()), attributes);
        }
        return map();
    }

    private Map<String, Object> positions() {
        return map("size", 10, "x", WIDTH/2 - Math.random() * WIDTH, "y", HEIGHT/2 - Math.random() * HEIGHT);
    }

    private Map<String, Object> color(String type, Map<String, Map<String, Object>> colors) {
        return colors.computeIfAbsent(type, k -> {
            int rgb = type.hashCode();
            float r = (float)(rgb >> 16 & 0xFF)/255.0f;
            float g = (float)(rgb >> 8  & 0xFF)/255.0f;
            float b = (float)(rgb       & 0xFF)/255.0f;
            return map("r", r, "g", g, "b", b);
        });
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
                if (key.toLowerCase().contains(caption)) return n.getProperty(key).toString();
            }
        }
        return first == null ? idStr(n) : n.getProperty(first).toString();
    }
}
