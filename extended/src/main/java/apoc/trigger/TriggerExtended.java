package apoc.trigger;

import apoc.Description;
import apoc.Extended;
import apoc.coll.SetBackedList;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author mh
 * @since 20.09.16
 */
@Extended
public class
TriggerExtended {
    public static class TriggerInfo {
        public String name;
        public String query;
        public Map<String,Object> selector;
        public Map<String, Object> params;
        public boolean installed;
        public boolean paused;

        public TriggerInfo(String name, String query, Map<String, Object> selector, boolean installed, boolean paused) {
            this.name = name;
            this.query = query;
            this.selector = selector;
            this.installed = installed;
            this.paused = paused;
        }

        public TriggerInfo( String name, String query, Map<String,Object> selector, Map<String,Object> params, boolean installed, boolean paused )
        {
            this.name = name;
            this.query = query;
            this.selector = selector;
            this.params = params;
            this.installed = installed;
            this.paused = paused;
        }
    }

    @Context public GraphDatabaseService db;

    @UserFunction
    @Description("function to filter labelEntries by label, to be used within a trigger kernelTransaction with {assignedLabels}, {removedLabels}, {assigned/removedNodeProperties}")
    public List<Node> nodesByLabel(@Name("labelEntries") Object entries, @Name("label") String labelString) {
        if (!(entries instanceof Map)) return Collections.emptyList();
        Map map = (Map) entries;
        if (map.isEmpty()) return Collections.emptyList();
        Object result = ((Map) entries).get(labelString);
        if (result instanceof List) return (List<Node>) result;
        Object anEntry = map.values().iterator().next();

        if (anEntry instanceof List) {
            List list = (List) anEntry;
            if (!list.isEmpty()) {
                if (list.get(0) instanceof Map) {
                    Set<Node> nodeSet = new HashSet<>(100);
                    Label label = labelString == null ? null : Label.label(labelString);
                    for (List<Map<String,Object>> entry : (Collection<List<Map<String,Object>>>) map.values()) {
                        for (Map<String, Object> propertyEntry : entry) {
                            Object node = propertyEntry.get("node");
                            if (node instanceof Node && (label == null || ((Node)node).hasLabel(label))) {
                                nodeSet.add((Node)node);
                            }
                        }
                    }
                    if (!nodeSet.isEmpty()) return new SetBackedList<>(nodeSet);
                } else if (list.get(0) instanceof Node) {
                    if (labelString==null) {
                        Set<Node> nodeSet = new HashSet<>(map.size()*list.size());
                        map.values().forEach((l) -> nodeSet.addAll((Collection<Node>)l));
                        return new SetBackedList<>(nodeSet);
                    }
                }
            }
        }
        return Collections.emptyList();
    }

    @UserFunction
    @Description("function to filter propertyEntries by property-key, to be used within a trigger kernelTransaction with {assignedNode/RelationshipProperties} and {removedNode/RelationshipProperties}. Returns [{old,new,key,node,relationship}]")
    public List<Map<String,Object>> propertiesByKey(@Name("propertyEntries") Map<String,List<Map<String,Object>>> propertyEntries, @Name("key") String key) {
        return propertyEntries.getOrDefault(key,Collections.emptyList());
    }

    public TriggerInfo toTriggerInfo(Map.Entry<String, Object> e) {
        String name = e.getKey();
        if (e.getValue() instanceof Map) {
            try {
                Map<String, Object> value = (Map<String, Object>) e.getValue();
                return new TriggerInfo(name, (String) value.get("statement"), (Map<String, Object>) value.get("selector"), (Map<String, Object>) value.get("params"), false, false);
            } catch(Exception ex) {
                return new TriggerInfo(name, ex.getMessage(), null, false, false);
            }
        }
        return new TriggerInfo(name, null, null, false, false);
    }
    
    @UserFunction
    @Description("apoc.trigger.toNode(node, $removedLabels, $removedNodeProperties) | function to rebuild a node as a virtual, to be used in triggers with a not 'afterAsync' phase")
    public Node toNode(@Name("id") Node node, @Name("removedLabels") Map<String, List<Node>> removedLabels, @Name("removedNodeProperties") Map<String, List<Map>> removedNodeProperties) {

        final long id = node.getId();
        final Label[] labels = removedLabels.entrySet().stream()
                .filter(i -> i.getValue().stream().anyMatch(l -> l.getId() == id))
                .map(e -> Label.label(e.getKey()))
                .toArray(Label[]::new);

        final Map<String, Object> props = removedNodeProperties.entrySet().stream()
                .map(i -> i.getValue().stream()
                        .filter(l -> ((Node) l.get("node")).getId() == id)
                        .findAny()
                        .map(v -> new AbstractMap.SimpleEntry<>(i.getKey(), v.get("old"))))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

        return new VirtualNode(labels, props);
    }
    
    @UserFunction
    @Description("apoc.trigger.toRelationship(rel, $removedRelationshipProperties) | function to rebuild a relationship as a virtual, to be used in triggers with a not 'afterAsync' phase")
    public Relationship toRelationship(@Name("id") Relationship rel, @Name("removedRelationshipProperties") Map<String, List<Map>> removedRelationshipProperties) {
        final Map<String, Object> props = removedRelationshipProperties.entrySet().stream()
                .map(i -> i.getValue().stream()
                        .filter(l -> ((Relationship) l.get("relationship")).getId() == rel.getId())
                        .findAny()
                        .map(v -> new AbstractMap.SimpleEntry<>(i.getKey(), v.get("old"))))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
        
        return new VirtualRelationship(rel.getStartNode(), rel.getEndNode(), rel.getType(), props);
    }
}
