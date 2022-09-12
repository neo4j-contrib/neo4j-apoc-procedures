package apoc.trigger;

import apoc.ApocConfig;
import apoc.Pools;
import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.Util;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static apoc.ApocConfig.APOC_TRIGGER_ENABLED;
import static apoc.ApocConfig.apocConfig;


class TriggerUtils {
    private final ApocConfig apocConfig = ApocConfig.apocConfig();

    public static final String NOT_ENABLED_ERROR = "Triggers have not been enabled." +
                                                   " Set 'apoc.trigger.enabled=true' in your apoc.conf file located in the $NEO4J_HOME/conf/ directory.";

    static Map<String, Object> toTriggerInfo(Node node) {
        HashSet<String> nodeKeys = new HashSet();
        node.getPropertyKeys().iterator().forEachRemaining(key -> nodeKeys.add( key ));
        HashMap<String, Object> result = new HashMap();

        if (nodeKeys.contains("statement"))
        {
            result.put( "statement", node.getProperty( SystemPropertyKeys.statement.name() ) );
        }
        if (nodeKeys.contains("selector"))
        {
            result.put( "selector", Util.fromJson((String) node.getProperty(SystemPropertyKeys.selector.name()), Map.class));
        }
        if (nodeKeys.contains("params"))
        {
            result.put( "params", Util.fromJson((String) node.getProperty(SystemPropertyKeys.params.name()), Map.class));
        }
        if (nodeKeys.contains("paused"))
        {
            result.put( "paused", node.getProperty(SystemPropertyKeys.paused.name()));
        }

        return result;
    }

    private boolean isEnabled() {
        return apocConfig.getBoolean(APOC_TRIGGER_ENABLED);
    }

    public void checkEnabled() {
        if (!isEnabled()) {
            throw new RuntimeException(NOT_ENABLED_ERROR);
        }
    }

    public Map<String, Object> add(String databaseName, String triggerName, String statement, Map<String,Object> selector, Map<String,Object> params) {
        checkEnabled();
        HashMap<String, Object> previous = new HashMap();

        withSystemDb(tx -> {
            Node node = Util.mergeNode(tx, SystemLabels.ApocTrigger, null,
                                       Pair.of(SystemPropertyKeys.database.name(), databaseName),
                                       Pair.of(SystemPropertyKeys.name.name(), triggerName));
            previous.putAll(TriggerUtils.toTriggerInfo(node));
            node.setProperty(SystemPropertyKeys.statement.name(), statement);
            node.setProperty(SystemPropertyKeys.selector.name(), Util.toJson(selector));
            node.setProperty(SystemPropertyKeys.params.name(), Util.toJson(params));
            node.setProperty(SystemPropertyKeys.paused.name(), false);
            setLastUpdate(databaseName, tx);
            return null;
        });

        return previous;
    }

    public Map<String, Object> remove(String databaseName, String triggerName) {
        checkEnabled();
        HashMap<String, Object> previous = new HashMap();

        withSystemDb(tx -> {
            tx.findNodes(SystemLabels.ApocTrigger,
                         SystemPropertyKeys.database.name(), databaseName,
                         SystemPropertyKeys.name.name(), triggerName)
              .forEachRemaining(node ->
                                {
                                    previous.putAll(TriggerUtils.toTriggerInfo(node));
                                    node.delete();
                                }
              );
            setLastUpdate(databaseName, tx);

            return null;
        });

        return previous;
    }

    public Map<String, Object> updatePaused(String databaseName, String name, boolean paused) {
        checkEnabled();
        HashMap<String, Object> result = new HashMap();

        withSystemDb(tx -> {
            tx.findNodes(SystemLabels.ApocTrigger,
                         SystemPropertyKeys.database.name(), databaseName,
                         SystemPropertyKeys.name.name(), name)
              .forEachRemaining(node ->
                                {
                                    node.setProperty( SystemPropertyKeys.paused.name(), paused );
                                    result.putAll(TriggerUtils.toTriggerInfo(node));
                                });
            setLastUpdate(databaseName, tx);

            return null;
        });

        return result;
    }

    public Map<String, Object> removeAll(String databaseName) {
        checkEnabled();
        HashMap<String, Object> previous = new HashMap();

        withSystemDb(tx -> {
            tx
                    .findNodes(SystemLabels.ApocTrigger,
                         SystemPropertyKeys.database.name(), databaseName )
                    .forEachRemaining(node -> {
                        String triggerName = (String) node.getProperty(SystemPropertyKeys.name.name());
                        previous.put(triggerName, TriggerUtils.toTriggerInfo(node));
                        node.delete();
                    });
            setLastUpdate(databaseName, tx);

            return null;
        });

        return previous;
    }

    public Map<String,Map<String,Object>> list(String databaseName) {
        checkEnabled();
        HashMap<String, Map<String, Object>> result = new HashMap();

        withSystemDb(tx -> {
                         tx
                                 .findNodes( SystemLabels.ApocTrigger,
                                             SystemPropertyKeys.database.name(), databaseName )
                                 .forEachRemaining( node ->
                                                    {
                                                        String triggerName = (String) node.getProperty( SystemPropertyKeys.name.name() );
                                                        result.put( triggerName, TriggerUtils.toTriggerInfo( node ) );
                                                    } );
                         return null;
                     });
        return result;
    }

    private <T> T withSystemDb(Function<Transaction, T> action) {
        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
            T result = action.apply(tx);
            tx.commit();
            return result;
        }
    }

    private void setLastUpdate(String databaseName, Transaction tx) {
        Node node = tx.findNode(SystemLabels.ApocTriggerMeta, SystemPropertyKeys.database.name(), databaseName);
        if (node == null) {
            node = tx.createNode(SystemLabels.ApocTriggerMeta);
            node.setProperty(SystemPropertyKeys.database.name(), databaseName);
        }
        node.setProperty(SystemPropertyKeys.lastUpdated.name(), System.currentTimeMillis());
    }
}


public class TriggerHandler extends LifecycleAdapter implements TransactionEventListener<Void> {

    private enum Phase {before, after, rollback, afterAsync}

    public static final String TRIGGER_REFRESH = "apoc.trigger.refresh";

    private final ConcurrentHashMap<String, Map<String,Object>> activeTriggers = new ConcurrentHashMap();
    private final Log log;
    private final GraphDatabaseService db;
    private final DatabaseManagementService databaseManagementService;
    private final ApocConfig apocConfig;
    private final Pools pools;
    private final JobScheduler jobScheduler;

    private long lastUpdate;

    private JobHandle restoreTriggerHandler;

    private final AtomicBoolean registeredWithKernel = new AtomicBoolean(false);

    public static final String NOT_ENABLED_ERROR = "Triggers have not been enabled." +
            " Set 'apoc.trigger.enabled=true' in your apoc.conf file located in the $NEO4J_HOME/conf/ directory.";

    public TriggerHandler(GraphDatabaseService db, DatabaseManagementService databaseManagementService,
                          ApocConfig apocConfig, Log log, Pools pools, JobScheduler jobScheduler) {
        this.db = db;
        this.databaseManagementService = databaseManagementService;
        this.apocConfig = apocConfig;
        this.log = log;
        this.pools = pools;
        this.jobScheduler = jobScheduler;
    }

    private void updateCache() {
        activeTriggers.clear();
        System.out.println("updates cache");
        lastUpdate = System.currentTimeMillis();

        withSystemDb(tx -> {
            tx.findNodes(SystemLabels.ApocTrigger,
                    SystemPropertyKeys.database.name(), db.databaseName()).forEachRemaining(
                    node -> {
                        System.out.println("there are nodes to update the cache");
                        activeTriggers.put(
                                (String) node.getProperty(SystemPropertyKeys.name.name()),
                                TriggerUtils.toTriggerInfo(node)
                        );
                    }
            );
            return null;
        });

        reconcileKernelRegistration();
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

        // TODO Nacho Find a solution to this, no longer valid in this mechanism
        //if (activeTriggers.size() > 0) {
            // This gets called every time triggers update; only register if we aren't already
            if(registeredWithKernel.compareAndSet(false, true)) {
                databaseManagementService.registerTransactionEventListener(db.databaseName(), this);
            }
//        } else {
//            // This gets called every time triggers update; only unregister if we aren't already
//            if(registeredWithKernel.compareAndSet(true, false)) {
//                databaseManagementService.unregisterTransactionEventListener(db.databaseName(), this);
//            }
//        }
    }

    @Override
    public Void beforeCommit(TransactionData txData, Transaction transaction, GraphDatabaseService databaseService) {
        if (hasPhase(Phase.before)) {
            executeTriggers(transaction, txData, Phase.before);
        }
        return null;
    }

    @Override
    public void afterCommit(TransactionData txData, Void state, GraphDatabaseService databaseService) {
        if (hasPhase(Phase.after)) {
            try (Transaction tx = db.beginTx()) {
                executeTriggers(tx, txData, Phase.after);
                tx.commit();
            }
        }
        afterAsync(txData);
    }

    private void afterAsync(TransactionData txData) {
        if (hasPhase(Phase.afterAsync)) {
            TriggerMetadata triggerMetadata = TriggerMetadata.from(txData, true);
            Util.inTxFuture(pools.getDefaultExecutorService(), db, (inner) -> {
                executeTriggers(inner, triggerMetadata.rebind(inner), Phase.afterAsync);
                return null;
            });
        }
    }

    @Override
    public void afterRollback(TransactionData txData, Void state, GraphDatabaseService databaseService) {
        if (hasPhase(Phase.rollback)) {
            try (Transaction tx = db.beginTx()) {
                executeTriggers(tx, txData, Phase.rollback);
                tx.commit();
            }
        }
    }

    private boolean hasPhase(Phase phase) {
        return activeTriggers.values().stream()
                .map(data -> (Map<String, Object>) data.get("selector"))
                .anyMatch(selector -> when(selector, phase));
    }

    private void executeTriggers(Transaction tx, TransactionData txData, Phase phase) {
        executeTriggers(tx, TriggerMetadata.from(txData, false), phase);
    }

    private void executeTriggers(Transaction tx, TriggerMetadata triggerMetadata, Phase phase) {
        Map<String,String> exceptions = new LinkedHashMap<>();
        activeTriggers.forEach((name, data) -> {
            Map<String, Object> params = triggerMetadata.toMap();
            if (data.get("params") != null) {
                params.putAll((Map<String, Object>) data.get("params"));
            }
            Map<String, Object> selector = (Map<String, Object>) data.get("selector");
            if ((!(boolean)data.get("paused")) && when(selector, phase)) {
                try {
                    params.put("trigger", name);
                    Result result = tx.execute((String) data.get("statement"), params);
                    Iterators.count(result);
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

    private boolean when(Map<String, Object> selector, Phase phase) {
        if (selector == null) return phase == Phase.before;
        return Phase.valueOf(selector.getOrDefault("phase", "before").toString()) == phase;
    }

    @Override
    public void start() throws Exception {
        updateCache();
        long refreshInterval = apocConfig().getInt(TRIGGER_REFRESH, 150);
        restoreTriggerHandler = jobScheduler.scheduleRecurring(Group.STORAGE_MAINTENANCE, () -> {
            System.out.println("excecuting job with lastUpdate: " + lastUpdate);
            if (getLastUpdate() > lastUpdate) {
                System.out.println("updating cache");
                updateCache();
            }
        }, refreshInterval, refreshInterval, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        if(registeredWithKernel.compareAndSet(true, false)) {
            databaseManagementService.unregisterTransactionEventListener(db.databaseName(), this);
        }
        if (restoreTriggerHandler != null) {
            restoreTriggerHandler.cancel();
        }
    }

    private <T> T withSystemDb(Function<Transaction, T> action) {
        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
            T result = action.apply(tx);
            tx.commit();
            return result;
        }
    }

    private long getLastUpdate() {
        return withSystemDb( tx -> {
            Node node = tx.findNode(SystemLabels.ApocTriggerMeta, SystemPropertyKeys.database.name(), db.databaseName());
            return node == null ? 0L : (long) node.getProperty(SystemPropertyKeys.lastUpdated.name());
        });
    }

}
