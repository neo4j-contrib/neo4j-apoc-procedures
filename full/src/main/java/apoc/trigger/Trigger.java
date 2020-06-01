package apoc.trigger;

import apoc.Description;
import apoc.coll.SetBackedList;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.stream.Stream;

/**
 * @author mh
 * @since 20.09.16
 */
public class Trigger {
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

    @Context public TriggerHandler triggerHandler;

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

    @Procedure(mode = Mode.WRITE)
    @Description("add a trigger kernelTransaction under a name, in the kernelTransaction you can use {createdNodes}, {deletedNodes} etc., the selector is {phase:'before/after/rollback'} returns previous and new trigger information. Takes in an optional configuration.")
    public Stream<TriggerInfo> add(@Name("name") String name, @Name("kernelTransaction") String statement, @Name(value = "selector"/*, defaultValue = "{}"*/)  Map<String,Object> selector, @Name(value = "config", defaultValue = "{}") Map<String,Object> config) {
        Map<String,Object> params = (Map)config.getOrDefault("params", Collections.emptyMap());
        Map<String, Object> removed = triggerHandler.add(name, statement, selector, params);
        if (removed != null) {
            return Stream.of(
                    new TriggerInfo(name,(String)removed.get("statement"), (Map<String, Object>) removed.get("selector"), (Map<String, Object>) removed.get("params"),false, false),
                    new TriggerInfo(name,statement,selector, params,true, false));
        }
        return Stream.of(new TriggerInfo(name,statement,selector, params,true, false));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("remove previously added trigger, returns trigger information")
    public Stream<TriggerInfo> remove(@Name("name")String name) {
        Map<String, Object> removed = triggerHandler.remove(name);
        if (removed == null) {
            return Stream.of(new TriggerInfo(name, null, null, false, false));
        }
        return Stream.of(new TriggerInfo(name,(String)removed.get("statement"), (Map<String, Object>) removed.get("selector"), (Map<String, Object>) removed.get("params"),false, false));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("removes all previously added trigger, returns trigger information")
    public Stream<TriggerInfo> removeAll() {
        Map<String, Object> removed = triggerHandler.removeAll();
        if (removed == null) {
            return Stream.of(new TriggerInfo(null, null, null, false, false));
        }
        return removed.entrySet().stream().map(this::toTriggerInfo);
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

    @Procedure(mode = Mode.WRITE)
    @Description("list all installed triggers")
    public Stream<TriggerInfo> list() {
        return triggerHandler.list().entrySet().stream()
                .map( (e) -> new TriggerInfo(e.getKey(),
                        (String)e.getValue().get("statement"),
                        (Map<String,Object>)e.getValue().get("selector"),
                        (Map<String, Object>) e.getValue().get("params"),
                        true,
                        (Boolean) e.getValue().getOrDefault("paused", false))
                );
    }

    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.pause(name) | it pauses the trigger")
    public Stream<TriggerInfo> pause(@Name("name")String name) {
        Map<String, Object> paused = triggerHandler.updatePaused(name, true);

        return Stream.of(new TriggerInfo(name,
                (String)paused.get("statement"),
                (Map<String,Object>) paused.get("selector"),
                (Map<String,Object>) paused.get("params"),true, true));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.resume(name) | it resumes the paused trigger")
    public Stream<TriggerInfo> resume(@Name("name")String name) {
        Map<String, Object> resume = triggerHandler.updatePaused(name, false);

        return Stream.of(new TriggerInfo(name,
                (String)resume.get("statement"),
                (Map<String,Object>) resume.get("selector"),
                (Map<String,Object>) resume.get("params"),true, false));
    }

}
