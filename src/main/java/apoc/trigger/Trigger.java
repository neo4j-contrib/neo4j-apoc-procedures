package apoc.trigger;

import apoc.ApocConfiguration;
import apoc.Description;
import apoc.coll.SetBackedList;
import apoc.util.Util;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.core.GraphProperties;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
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

    @Procedure(mode = Mode.WRITE)
    @Description("add a trigger kernelTransaction under a name, in the kernelTransaction you can use {createdNodes}, {deletedNodes} etc., the selector is {phase:'before/after/rollback'} returns previous and new trigger information. Takes in an optional configuration.")
    public Stream<TriggerInfo> add(@Name("name") String name, @Name("kernelTransaction") String statement, @Name(value = "selector"/*, defaultValue = "{}"*/)  Map<String,Object> selector, @Name(value = "config", defaultValue = "{}") Map<String,Object> config) {
        Map<String,Object> params = (Map)config.getOrDefault("params", Collections.emptyMap());
        Map<String, Object> removed = TriggerHandler.getInstance().add(name, statement, selector, params);
        if (removed != null) {
            return Stream.of(
                    new TriggerInfo(name,(String)removed.get("kernelTransaction"), (Map<String, Object>) removed.get("selector"), (Map<String, Object>) removed.get("params"),false, false),
                    new TriggerInfo(name,statement,selector, params,true, false));
        }
        return Stream.of(new TriggerInfo(name,statement,selector, params,true, false));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("remove previously added trigger, returns trigger information")
    public Stream<TriggerInfo> remove(@Name("name")String name) {
        Map<String, Object> removed = TriggerHandler.getInstance().remove(name);
        if (removed == null) {
            return Stream.of(new TriggerInfo(name, null, null, false, false));
        }
        return Stream.of(new TriggerInfo(name,(String)removed.get("kernelTransaction"), (Map<String, Object>) removed.get("selector"), (Map<String, Object>) removed.get("params"),false, false));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("removes all previously added trigger, returns trigger information")
    public Stream<TriggerInfo> removeAll() {
        Map<String, Object> removed = TriggerHandler.getInstance().removeAll();
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
                return new TriggerInfo(name, (String) value.get("kernelTransaction"), (Map<String, Object>) value.get("selector"), (Map<String, Object>) value.get("params"), false, false);
            } catch(Exception ex) {
                return new TriggerInfo(name, ex.getMessage(), null, false, false);
            }
        }
        return new TriggerInfo(name, null, null, false, false);
    }

    @Procedure(mode = Mode.WRITE)
    @Description("list all installed triggers")
    public Stream<TriggerInfo> list() {
        return TriggerHandler.getInstance().list().entrySet().stream()
                .map( (e) -> new TriggerInfo(e.getKey(),(String)e.getValue().get("kernelTransaction"),(Map<String,Object>)e.getValue().get("selector"), (Map<String, Object>) e.getValue().get("params"),true, (Boolean) e.getValue().get("paused")));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.pause(name) | it pauses the trigger")
    public Stream<TriggerInfo> pause(@Name("name")String name) {
        Map<String, Object> paused = TriggerHandler.getInstance().paused(name);

        return Stream.of(new TriggerInfo(name,(String)paused.get("kernelTransaction"), (Map<String,Object>) paused.get("selector"), (Map<String,Object>) paused.get("params"),true, true));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.resume(name) | it resumes the paused trigger")
    public Stream<TriggerInfo> resume(@Name("name")String name) {
        Map<String, Object> resume = TriggerHandler.getInstance().resume(name);

        return Stream.of(new TriggerInfo(name,(String)resume.get("kernelTransaction"), (Map<String,Object>) resume.get("selector"), (Map<String,Object>) resume.get("params"),true, false));
    }

    public static class TriggerHandler implements TransactionEventHandler {
        public static final String APOC_TRIGGER = "apoc.trigger";
        private final ConcurrentHashMap<String,Map<String,Object>> triggers = new ConcurrentHashMap(map("",map()));
        private final GraphProperties properties;
        private final Log log;

        public static final String NOT_ENABLED_ERROR = "Triggers have not been enabled." +
                " Set 'apoc.trigger.enabled=true' in your neo4j.conf file located in the $NEO4J_HOME/conf/ directory.";
        private final GraphDatabaseService db;
        private final boolean enabled;
        private final AtomicBoolean registeredWithKernel = new AtomicBoolean(false);

        private static TriggerHandler instance;

        private TriggerHandler(GraphDatabaseAPI api, Log log, boolean enabled) {
            properties = api.getDependencyResolver().resolveDependency(EmbeddedProxySPI.class).newGraphPropertiesProxy();
//            Pools.SCHEDULED.submit(() -> updateTriggers(null,null));
            this.log = log;
            this.db = api;
            this.enabled = enabled;
        }

        public static TriggerHandler initialize(GraphDatabaseAPI api, Log log, boolean enabled) {
            instance = new TriggerHandler(api, log, enabled);
            return instance;
        }

        public static TriggerHandler getInstance() {
            if (instance == null) {
                throw new IllegalStateException("TriggerHandler has not yet been initialized");
            }
            return instance;
        }

        public void shutdown() {
            if (registeredWithKernel.compareAndSet(true, false)) {
                db.unregisterTransactionEventHandler(this);
            }
            instance = null;
        }

        public void checkEnabled() {
            if (!enabled) {
                throw new RuntimeException(NOT_ENABLED_ERROR);
            }
        }

        public Map<String, Object> add(String name, String statement, Map<String,Object> selector) {
            checkEnabled();

            return add(name, statement, selector, Collections.emptyMap());
        }

        public Map<String, Object> add(String name, String statement, Map<String,Object> selector, Map<String,Object> params) {
            checkEnabled();

            return updateTriggers(name, map("kernelTransaction", statement, "selector", selector, "params", params, "paused", false));
        }

        public synchronized Map<String, Object> remove(String name) {
            return updateTriggers(name,null);
        }

        public Map<String, Object> paused(String name) {
            checkEnabled();

            Map<String, Object> triggerToPause = triggers.get(name);
            updateTriggers(name, map("kernelTransaction", triggerToPause.get("kernelTransaction"), "selector", triggerToPause.get("selector"), "params", triggerToPause.get("params"), "paused", true));
            return triggers.get(name);
        }

        public Map<String, Object> resume(String name) {
            checkEnabled();

            Map<String, Object> triggerToResume = triggers.get(name);
            updateTriggers(name, map("kernelTransaction", triggerToResume.get("kernelTransaction"), "selector", triggerToResume.get("selector"), "params", triggerToResume.get("params"), "paused", false));
            return triggers.get(name);
        }

        private synchronized Map<String, Object> updateTriggers(String name, Map<String, Object> value) {
            checkEnabled();

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
                reconcileKernelRegistration();
                return previous;
            }
        }

        /**
         * There is substantial memory overhead to the kernel event system, so if a user has enabled apoc triggers in
         * config, but there are no triggers set up, unregister to let the kernel bypass the event handling system.
         *
         * For most deployments this isn't an issue, since you can turn the config flag off, but in large fleet deployments
         * it's nice to have uniform config, and then the memory savings on databases that don't use triggers is good.
         */
        private synchronized void reconcileKernelRegistration() {
            // Register if there are triggers
            if (triggers.size() > 0) {
                // This gets called every time triggers update; only register if we aren't already
                if(registeredWithKernel.compareAndSet(false, true)) {
                    db.registerTransactionEventHandler(this);
                }
            } else {
                // This gets called every time triggers update; only unregister if we aren't already
                if(registeredWithKernel.compareAndSet(true, false)) {
                    db.unregisterTransactionEventHandler(this);
                }
            }
        }

        public synchronized Map<String, Object> removeAll() {
            try (Transaction tx = properties.getGraphDatabase().beginTx()) {
                triggers.clear();
                String previous = (String) properties.removeProperty(APOC_TRIGGER);
                tx.success();
                return previous == null ? null : Util.fromJson(previous, Map.class);
            } catch (Exception e) {
                return null;
            }
        }

        public Map<String,Map<String,Object>> list() {
            checkEnabled();

            updateTriggers(null,null);
            return triggers;
        }

        @Override
        public Object beforeCommit(TransactionData txData) throws Exception {
            executeTriggers(txData, "before");
            return null;
        }

        private void executeTriggers(TransactionData txData, String phase) {
            if (triggers.containsKey("")) updateTriggers(null,null);
            GraphDatabaseService db = properties.getGraphDatabase();
            Map<String,String> exceptions = new LinkedHashMap<>();
            Map<String, Object> params = txDataParams(txData, phase);
            triggers.forEach((name, data) -> {
                if( data.get("paused").equals(false)) {
                    if( data.get( "params" ) != null)
                    {
                        params.putAll( (Map<String,Object>) data.get( "params" ) );
                    }
                    try (Transaction tx = db.beginTx()) {
                        Map<String,Object> selector = (Map<String, Object>) data.get("selector");
                        if (when(selector, phase)) {
                            params.put("trigger", name);
                            Result result = db.execute((String) data.get("kernelTransaction"), params);
                            Iterators.count(result);
                            result.close();
                        }
                        tx.success();
                    } catch(Exception e) {
                        log.warn("Error executing trigger "+name+" in phase "+phase,e);
                        exceptions.put(name, e.getMessage());
                    }
                }
            });
            if (!exceptions.isEmpty()) {
                throw new RuntimeException("Error executing triggers "+exceptions.toString());
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

    private static Map<String, Object> txDataParams(TransactionData txData, String phase) {
        return map("transactionId", phase.equals("after") ? txData.getTransactionId() : -1,
                        "commitTime", phase.equals("after") ? txData.getCommitTime() : -1,
                        "createdNodes", txData.createdNodes(),
                        "createdRelationships", txData.createdRelationships(),
                        "deletedNodes", txData.deletedNodes(),
                        "deletedRelationships", txData.deletedRelationships(),
                        "removedLabels", aggregateLabels(txData.removedLabels()),
                        "removedNodeProperties", aggregatePropertyKeys(txData.removedNodeProperties(),true,true),
                        "removedRelationshipProperties", aggregatePropertyKeys(txData.removedRelationshipProperties(),false,true),
                        "assignedLabels", aggregateLabels(txData.assignedLabels()),
                        "assignedNodeProperties",aggregatePropertyKeys(txData.assignedNodeProperties(),true,false),
                        "assignedRelationshipProperties",aggregatePropertyKeys(txData.assignedRelationshipProperties(),false,false));
    }

    private static <T extends PropertyContainer> Map<String,List<Map<String,Object>>> aggregatePropertyKeys(Iterable<PropertyEntry<T>> entries, boolean nodes, boolean removed) {
        if (!entries.iterator().hasNext()) return Collections.emptyMap();
        Map<String,List<Map<String,Object>>> result = new HashMap<>();
        String entityType = nodes ? "node" : "relationship";
        for (PropertyEntry<T> entry : entries) {
            result.compute(entry.key(),
                    (k, v) -> {
                        if (v == null) v = new ArrayList<>(100);
                        Map<String, Object> map = map("key", k, entityType, entry.entity(), "old", entry.previouslyCommitedValue());
                        if (!removed) map.put("new", entry.value());
                        v.add(map);
                        return v;
                    });
        }
        return result;
    }
    private static Map<String,List<Node>> aggregateLabels(Iterable<LabelEntry> labelEntries) {
        if (!labelEntries.iterator().hasNext()) return Collections.emptyMap();
        Map<String,List<Node>> result = new HashMap<>();
        for (LabelEntry entry : labelEntries) {
            result.compute(entry.label().name(),
                    (k, v) -> {
                        if (v == null) v = new ArrayList<>(100);
                        v.add(entry.node());
                        return v;
                    });
        }
        return result;
    }

    public static class LifeCycle implements AvailabilityListener {
        private final GraphDatabaseAPI db;
        private final Log log;
        private boolean enabled;

        public LifeCycle(GraphDatabaseAPI db, Log log) {
            this.db = db;
            this.log = log;
        }

        public void start() {
            enabled = Util.toBoolean(ApocConfiguration.get("trigger.enabled", null));
            TriggerHandler.initialize(db, log, enabled);
        }

        public void stop() {
            TriggerHandler.getInstance().shutdown();
        }

        @Override
        public void available() {
            if (enabled) {
                TriggerHandler.getInstance().updateTriggers(null, null);
            }
        }

        @Override
        public void unavailable() {

        }
    }
}
