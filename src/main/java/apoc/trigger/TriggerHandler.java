package apoc.trigger;

import apoc.ApocConfig;
import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.Util;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;
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
    private final ThrowingFunction<Context, Transaction, ProcedureException> transactionComponentFunction;

    public TriggerHandler(GraphDatabaseService db, DatabaseManagementService databaseManagementService,
                          ApocConfig apocConfig, Log log, GlobalProceduresRegistry globalProceduresRegistry) {
        this.db = db;
        this.databaseManagementService = databaseManagementService;
        this.apocConfig = apocConfig;
        this.log = log;
        transactionComponentFunction = globalProceduresRegistry.lookupComponentProvider(Transaction.class, true);
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
        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
            tx.findNodes(SystemLabels.ApocTrigger,
                    SystemPropertyKeys.database.name(), db.databaseName()).forEachRemaining(
                node -> {
                    activeTriggers.put(
                            (String) node.getProperty(SystemPropertyKeys.name.name()),
                            MapUtil.map(
                                    "statement", node.getProperty(SystemPropertyKeys.statement.name()),
                                    "selector", Util.fromJson((String) node.getProperty(SystemPropertyKeys.selector.name()), Map.class),
                                    "params", Util.fromJson((String) node.getProperty(SystemPropertyKeys.params.name()), Map.class),
                                    "paused", node.getProperty(SystemPropertyKeys.paused.name())
                            )
                    );

                }
            );

            tx.commit();
        }
    }

    public Map<String, Object> add(String name, String statement, Map<String,Object> selector) {
        return add(name, statement, selector, Collections.emptyMap());
    }

    public Map<String, Object> add(String name, String statement, Map<String,Object> selector, Map<String,Object> params) {
        checkEnabled();
        Map<String, Object> previous = activeTriggers.get(name);

        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
            Node node = Util.mergeNode(tx, SystemLabels.ApocTrigger, null,
                    Pair.of(SystemPropertyKeys.database.name(), db.databaseName()),
                    Pair.of(SystemPropertyKeys.name.name(), name));
            node.setProperty(SystemPropertyKeys.statement.name(), statement);
            node.setProperty(SystemPropertyKeys.selector.name(), Util.toJson(selector));
            node.setProperty(SystemPropertyKeys.params.name(), Util.toJson(params));
            node.setProperty(SystemPropertyKeys.paused.name(), false);
            tx.commit();
        }
        updateCache();
        return previous;
    }

    public Map<String, Object> remove(String name) {
        checkEnabled();
        Map<String, Object> previous = activeTriggers.remove(name);

        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
            tx.findNodes(SystemLabels.ApocTrigger,
                    SystemPropertyKeys.database.name(), db.databaseName(),
                    SystemPropertyKeys.name.name(), name)
                    .forEachRemaining(node -> node.delete());
            tx.commit();
        }
        updateCache();
        return previous;
    }

    public Map<String, Object> updatePaused(String name, boolean paused) {
        checkEnabled();
        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
            tx.findNodes(SystemLabels.ApocTrigger,
                    SystemPropertyKeys.database.name(), db.databaseName(),
                    SystemPropertyKeys.name.name(), name)
                    .forEachRemaining(node -> node.setProperty(SystemPropertyKeys.paused.name(), paused));
            tx.commit();
        }
        updateCache();
        return activeTriggers.get(name);
    }

    public Map<String, Object> removeAll() {
        checkEnabled();
        Map<String, Object> previous = activeTriggers
                .entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
            tx.findNodes(SystemLabels.ApocTrigger,
                    SystemPropertyKeys.database.name(), db.databaseName() )
                    .forEachRemaining(node -> node.delete());
            tx.commit();
        }
        updateCache();
        return previous;
    }

    public Map<String,Map<String,Object>> list() {
        checkEnabled();
        return activeTriggers;
    }

    @Override
    public Void beforeCommit(TransactionData txData, Transaction transaction, GraphDatabaseService databaseService) {
        executeTriggers(transaction, txData, "before");
        return null;
    }

    @Override
    public void afterCommit(TransactionData txData, Void state, GraphDatabaseService databaseService) {
        try (Transaction tx = db.beginTx()) {
            executeTriggers(tx, txData, "after");
            tx.commit();
        }
    }

    @Override
    public void afterRollback(TransactionData txData, Void state, GraphDatabaseService databaseService) {
        try (Transaction tx = db.beginTx()) {
            executeTriggers(tx, txData, "rollback");
            tx.commit();
        }
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

    private void executeTriggers(Transaction tx, TransactionData txData, String phase) {
        Map<String,String> exceptions = new LinkedHashMap<>();
        Map<String, Object> params = txDataParams(txData, phase);
        activeTriggers.forEach((name, data) -> {
            if (data.get("params") != null) {
                params.putAll((Map<String, Object>) data.get("params"));
            }
            Map<String, Object> selector = (Map<String, Object>) data.get("selector");
            if ((!(boolean)data.get("paused")) && when(selector, phase)) {

                try {
                    params.put("trigger", name);
                    Result result = tx.execute((String) data.get("statement"), params);
                    Iterators.count(result);
//                    result.close();
                } catch (Exception e) {
                    log.warn("Error executing trigger " + name + " in phase " + phase, e);
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
