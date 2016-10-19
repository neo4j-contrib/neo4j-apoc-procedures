package apoc.trigger;

import apoc.Description;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.kernel.impl.core.GraphProperties;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
//import org.neo4j.procedure.Mode;
import org.neo4j.procedure.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static apoc.util.Util.map;

/**
 * @author mh
 * @since 20.09.16
 */
public class Trigger {

    public static class TriggerInfo {
        public String name;
        public String query;
        public Map<String,Object> selector;
        public boolean installed;

        public TriggerInfo(String name, String query, Map<String, Object> selector, boolean installed) {
            this.name = name;
            this.query = query;
            this.selector = selector;
            this.installed = installed;
        }
    }

    @UserFunction
    @Description("function to filter labelEntries by label, to be used within a trigger statement with {assignedLabels} and {removedLabels}")
    public List<Node> nodesByLabel(@Name("labelEntries") Object labelEntries, @Name("label") String label) {
        if (!(labelEntries instanceof Iterable)) return Collections.emptyList();
        List<Node> nodes = null;
        for (LabelEntry labelEntry : (Iterable<LabelEntry>) labelEntries) {
            if (labelEntry.label().name().equals(label)) {
                if (nodes==null) nodes = new ArrayList<>(100);
                nodes.add(labelEntry.node());
            }
        }
        return nodes;
    }

    @UserFunction
    @Description("function to filter propertyEntries by property-key, to be used within a trigger statement with {assignedNode/RelationshipProperties} and {removedNode/RelationshipProperties}. Returns [{old,new,key,node,relationship}]")
    public List<Map<String,Object>> propertiesByKey(@Name("propertyEntries") Object propertyEntries, @Name("key") String key) {
        if (!(propertyEntries instanceof Iterable)) return Collections.emptyList();
        List<Map<String,Object>> result = null;
        for (PropertyEntry<?> entry : (Iterable<PropertyEntry<?>>) propertyEntries) {
            if (entry.key().equals(key)) {
                if (result==null) result = new ArrayList<>(100);
                PropertyContainer entity = entry.entity();
                Map<String, Object> map = map("old", entry.previouslyCommitedValue(), "new", entry.value(), "key", key, (entity instanceof Node ? "node" : "relationship"), entity);
                result.add(map);
            }
        }
        return result;
    }

    @Procedure(mode = Mode.WRITE)
    @Description("add a trigger statement under a name, in the statement you can use {createdNodes}, {deletedNodes} etc., the selector is {phase:'before/after/rollback'} returns previous and new trigger information")
    public Stream<TriggerInfo> add(@Name("name") String name, @Name("statement") String statement, @Name(value = "selector"/*, defaultValue = "{}"*/)  Map<String,Object> selector) {
        Map<String, Object> removed = TriggerHandler.add(name, statement, selector);
        if (removed != null) {
            return Stream.of(
                    new TriggerInfo(name,(String)removed.get("statement"), (Map<String, Object>) removed.get("selector"),false),
                    new TriggerInfo(name,statement,selector,true));
        }
        return Stream.of(new TriggerInfo(name,statement,selector,true));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("remove previously added trigger, returns trigger information")
    public Stream<TriggerInfo> remove(@Name("name")String name) {
        Map<String, Object> removed = TriggerHandler.remove(name);
        if (removed == null) {
            Stream.of(new TriggerInfo(name, null, null, false));
        }
        return Stream.of(new TriggerInfo(name,(String)removed.get("statement"), (Map<String, Object>) removed.get("selector"),false));
    }

    public static class TriggerHandler implements TransactionEventHandler {
        public static final String APOC_TRIGGER = "apoc.trigger";
        static ConcurrentHashMap<String,Map<String,Object>> triggers = new ConcurrentHashMap(map("",map()));
        private static GraphProperties properties;

        public TriggerHandler(GraphDatabaseAPI api) {
            properties = api.getDependencyResolver().resolveDependency(NodeManager.class).newGraphProperties();
//            Pools.SCHEDULED.submit(() -> updateTriggers(null,null));

        }

        public static Map<String, Object> add(String name, String statement, Map<String,Object> selector) {
            return updateTriggers(name, map("statement", statement, "selector", selector));
        }
        public synchronized static Map<String, Object> remove(String name) {
            return updateTriggers(name,null);
        }

        private synchronized static Map<String, Object> updateTriggers(String name, Map<String, Object> value) {
            try (Transaction tx = properties.getGraphDatabase().beginTx()) {
                triggers.clear();
                String triggerProperty = (String) properties.getProperty(APOC_TRIGGER, "{}");
                triggers.putAll(Util.fromJson(triggerProperty,Map.class));
                Map<String,Object> previous = null;
                if (name != null) {
                    previous = (value == null) ? triggers.remove(name) : triggers.put(name, value);
                    if (value != null || previous != null) {
                        properties.setProperty(APOC_TRIGGER, Util.toJson(triggers));
                    }
                }
                tx.success();
                return previous;
            }
        }

        @Override
        public Object beforeCommit(TransactionData txData) throws Exception {
            executeTriggers(txData, "before");
            return null;
        }

        private void executeTriggers(TransactionData txData, String phase) {
            Map<String, Object> params = map(
                    "transactionId", phase.equals("after") ? txData.getTransactionId() : -1,
                    "commitTime", phase.equals("after") ? txData.getCommitTime() : -1,
                    "createdNodes", txData.createdNodes(),
                    "createdRelationships", txData.createdRelationships(),
                    "deletedNodes", txData.deletedNodes(),
                    "deletedRelationships", txData.deletedRelationships(),
                    "removedLabels", txData.removedLabels(),
                    "removedNodeProperties", txData.removedNodeProperties(),
                    "removedRelationshipProperties", txData.removedRelationshipProperties(),
                    "assignedLabels",txData.assignedLabels(),
                    "assignedNodeProperties",txData.assignedNodeProperties(),
                    "assignedRelationshipProperties",txData.assignedRelationshipProperties());
            if (triggers.containsKey("")) updateTriggers(null,null);
            GraphDatabaseService db = properties.getGraphDatabase();
            try (Transaction tx = db.beginTx()) {
                triggers.forEach((name, data) -> {
                    Map<String,Object> selector = (Map<String, Object>) data.get("selector");
                    if (when(selector, phase)) {
                        params.put("trigger", name);
                        db.execute((String) data.get("statement"), params);
                    }
                });
                tx.success();
            }
        }

        private boolean when(Map<String, Object> selector, String phase) {
            if (selector == null) return (phase.equals("before"));
            return selector.getOrDefault("phase", "before").equals(phase);
        }

        @Override
        public void afterCommit(TransactionData txData, Object state) {
            executeTriggers(txData, "after");
        }

        @Override
        public void afterRollback(TransactionData txData, Object state) {
            executeTriggers(txData, "rollback");
        }
    }
}
