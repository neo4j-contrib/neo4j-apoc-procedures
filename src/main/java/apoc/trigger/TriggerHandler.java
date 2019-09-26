package apoc.trigger;

import apoc.ApocConfig;
import apoc.util.Util;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static apoc.ApocConfig.APOC_TRIGGER_ENABLED;
import static apoc.util.Util.map;

public class TriggerHandler extends LifecycleAdapter implements TransactionEventListener<Void> {

    private final ConcurrentHashMap<String, Map<String,Object>> activeTriggers = new ConcurrentHashMap();
    private final Log log;
    private final GraphDatabaseService db;
    private final DatabaseManagementService databaseManagementService;
    private final ApocConfig apocConfig;

    public static final String NOT_ENABLED_ERROR = "Triggers have not been enabled." +
            " Set 'apoc.trigger.enabled=true' in your apoc.conf file located in the $NEO4J_HOME/conf/ directory.";
    private static final Map<String, TriggerHandler> triggerLifecyclesByDatabaseName = new ConcurrentHashMap<>();

    public TriggerHandler(GraphDatabaseService db, DatabaseManagementService databaseManagementService,
                          ApocConfig apocConfig, Log log, GlobalProceduresRegistry globalProceduresRegistry) {
        this.db = db;
        this.databaseManagementService = databaseManagementService;
        this.apocConfig = apocConfig;
//            Pools.SCHEDULED.submit(() -> updateTriggers(null,null));
        this.log = log;
        triggerLifecyclesByDatabaseName.put(db.databaseName(), this);
        globalProceduresRegistry.registerComponent(TriggerHandler.class, ctx -> {
            String databaseName = ctx.graphDatabaseAPI().databaseName();
            return triggerLifecyclesByDatabaseName.get(databaseName);
        }, true);
    }

    private boolean isEnabled() {
        return apocConfig.getBoolean(APOC_TRIGGER_ENABLED);
    }

    public void checkEnabled() {
        if (!isEnabled()) {
            throw new RuntimeException(NOT_ENABLED_ERROR);
        }
    }

    private void updateCache() {
        activeTriggers.clear();
        apocConfig.getSystemDb().executeTransactionally("MATCH (trigger:ApocTrigger{database:$database, paused:false}) RETURN trigger",
                Collections.singletonMap("database", db.databaseName()),
                result -> {
                    result.columnAs("trigger").forEachRemaining(o -> {
                        Node node = (Node) o;
                        activeTriggers.put(
                                (String) node.getProperty("name"),
                                MapUtil.map(
                                        "statement", (String) node.getProperty("statement"),
                                        "selector", Util.fromJson((String) node.getProperty("selector"), Map.class),
                                        "params", Util.fromJson((String) node.getProperty("params"), Map.class)
                                )
                        );
                    });
                    return null;
                });
    }

    public Map<String, Object> add(String name, String statement, Map<String,Object> selector) {
        return add(name, statement, selector, Collections.emptyMap());
    }

    public Map<String, Object> add(String name, String statement, Map<String,Object> selector, Map<String,Object> params) {
        checkEnabled();

        Map<String, Object> previous = activeTriggers.get(name);
        Map<String, Object> cypherParams = MapUtil.map(
                "database", db.databaseName(),
                "statement", statement,
                "selector", Util.toJson(selector),
                "params", Util.toJson(params),
                "paused", false
        );
        apocConfig.getSystemDb().executeTransactionally("merge (trigger:ApocTrigger{database:$params.database, name:$params.name}) set trigger=$params", cypherParams);
        updateCache();
        return previous;
    }

    public Map<String, Object> remove(String name) {
        checkEnabled();
        Map<String, Object> previous = activeTriggers.get(name);
        Map<String, Object> params = MapUtil.map(
                "database", db.databaseName(),
                "name", name
        );
        apocConfig.getSystemDb().executeTransactionally("MATCH (trigger:ApocTrigger{database:$database, name:$name}) DELETE trigger", params);
        updateCache();
        return previous;
    }

    public Map<String, Object> updatePaused(String name, boolean paused) {
        checkEnabled();
        Map<String, Object> params = MapUtil.map(
                "database", db.databaseName(),
                "name", name,
                "paused", paused
        );
        apocConfig.getSystemDb().executeTransactionally("MATCH (trigger:ApocTrigger{database:$database, name:$name}) SET trigger.paused=$paused", params);
        updateCache();
        return activeTriggers.get(name);
    }

    public Map<String, Object> removeAll() {
        checkEnabled();
        Map<String, Object> previous = activeTriggers
                .entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        apocConfig.getSystemDb().executeTransactionally(
                "MATCH (trigger:ApocTrigger{database:$database}) DELETE trigger",
                Collections.singletonMap("database", db.databaseName())
        );
        updateCache();
        return previous;
    }

    public Map<String,Map<String,Object>> list() {
        return activeTriggers;
    }

    @Override
    public Void beforeCommit(TransactionData txData, GraphDatabaseService databaseService) {
        executeTriggers(txData, "before");
        return null;
    }

    @Override
    public void afterCommit(TransactionData txData, Void state, GraphDatabaseService databaseService) {
        executeTriggers(txData, "after");
    }

    @Override
    public void afterRollback(TransactionData txData, Void state, GraphDatabaseService databaseService) {
        executeTriggers(txData, "rollback");
    }

    static <T extends Entity> Map<String,List<Map<String,Object>>> aggregatePropertyKeys(Iterable<PropertyEntry<T>> entries, boolean nodes, boolean removed) {
        if (!entries.iterator().hasNext()) return Collections.emptyMap();
        Map<String,List<Map<String,Object>>> result = new HashMap<>();
        String entityType = nodes ? "node" : "relationship";
        for (PropertyEntry<T> entry : entries) {
            result.compute(entry.key(),
                    (k, v) -> {
                        if (v == null) v = new ArrayList<>(100);
                        Map<String, Object> map = map("key", k, entityType, entry.entity(), "old", entry.previouslyCommittedValue());
                        if (!removed) map.put("new", entry.value());
                        v.add(map);
                        return v;
                    });
        }
        return result;
    }

    private Map<String, List<Node>> aggregateLabels(Iterable<LabelEntry> labelEntries) {
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

    private Map<String, Object> txDataParams(TransactionData txData, String phase) {
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
    private void executeTriggers(TransactionData txData, String phase) {
        Map<String,String> exceptions = new LinkedHashMap<>();
        Map<String, Object> params = txDataParams(txData, phase);
        activeTriggers.forEach((name, data) -> {
            if( data.get("paused").equals(false)) {
                if( data.get( "params" ) != null)
                {
                    params.putAll( (Map<String,Object>) data.get( "params" ) );
                }
                try (Transaction tx = db.beginTx()) {
                    Map<String,Object> selector = (Map<String, Object>) data.get("selector");
                    if (when(selector, phase)) {
                        params.put("trigger", name);
                        Result result = tx.execute((String) data.get("kernelTransaction"), params);
                        Iterators.count(result);
                        result.close();
                    }
                    tx.commit();
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
    public void start() {
        if (isEnabled()) {
            databaseManagementService.registerTransactionEventListener(db.databaseName(), this);
        }
    }

    @Override
    public void stop() {
        if (isEnabled()) {
            databaseManagementService.unregisterTransactionEventListener(db.databaseName(), this);
        }
    }


}
