package apoc.trigger;

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
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static apoc.ApocConfig.apocConfig;
import static apoc.trigger.TriggerHandlerWrite.getTriggerNodes;
import static apoc.trigger.TriggerHandlerWrite.withSystemDb;


public class TriggerHandlerRead extends LifecycleAdapter implements TransactionEventListener<Void> {

    public static final int TRIGGER_RELOAD_DEFAULT_VALUE = 3000;
    public static final String TRIGGER_RELOAD = "apoc.trigger.reload";

    private enum Phase {before, after, rollback, afterAsync}
    
    private final ConcurrentHashMap<String, Map<String,Object>> activeTriggers = new ConcurrentHashMap<>();

    private final Log log;
    private final GraphDatabaseService db;
    private final DatabaseManagementService databaseManagementService;
    private final Pools pools;
    
    private final JobScheduler jobScheduler;

    private long lastUpdate;

    private JobHandle restoreTriggerHandler;

    private final AtomicBoolean registeredWithKernel = new AtomicBoolean(false);

    public TriggerHandlerRead(GraphDatabaseService db, DatabaseManagementService databaseManagementService,
                              Log log, Pools pools, JobScheduler jobScheduler) {
        this.db = db;
        this.databaseManagementService = databaseManagementService;
        this.log = log;
        this.pools = pools;
        this.jobScheduler = jobScheduler;
    }


    public void reconcileKernelRegistration() {
        final boolean hasTriggers = withSystemDb(tx -> getTriggerNodes(db.databaseName(), tx)
                .hasNext());
        reconcileKernelRegistration(hasTriggers);
    }

    /**
     * There is substantial memory overhead to the kernel event system, so if a user has enabled apoc triggers in
     * config, but there are no triggers set up, unregister to let the kernel bypass the event handling system.
     *
     * For most deployments this isn't an issue, since you can turn the config flag off, but in large fleet deployments
     * it's nice to have uniform config, and then the memory savings on databases that don't use triggers is good.
     */
    public synchronized void reconcileKernelRegistration(boolean hasTriggers) {
        // Register if there are triggers
        if (hasTriggers) {
            // This gets called every time triggers update; only register if we aren't already
            if(registeredWithKernel.compareAndSet(false, true)) {
                databaseManagementService.registerTransactionEventListener(db.databaseName(), this);
            }
        } else {
            // This gets called every time triggers update; only unregister if we aren't already
            if(registeredWithKernel.compareAndSet(true, false)) {
                databaseManagementService.unregisterTransactionEventListener(db.databaseName(), this);
            }
        }
    }

    public Map<String,Map<String,Object>> list() {
        TriggerHandlerWrite.checkEnabled();
        return Map.copyOf(activeTriggers);
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
            throw new RuntimeException("Error executing triggers "+ exceptions);
        }
    }

    private boolean when(Map<String, Object> selector, Phase phase) {
        if (selector == null) return phase == Phase.before;
        return Phase.valueOf(selector.getOrDefault("phase", "before").toString()) == phase;
    }

    @Override
    public void start() throws Exception {
        updateCache();
        long refreshInterval = apocConfig().getInt(TRIGGER_RELOAD, TRIGGER_RELOAD_DEFAULT_VALUE);
        restoreTriggerHandler = jobScheduler.scheduleRecurring(Group.STORAGE_MAINTENANCE, () -> {
            if (getLastUpdate() > lastUpdate) {
                updateCache();
            }
        }, refreshInterval, refreshInterval, TimeUnit.MILLISECONDS);
    }

    private long getLastUpdate() {
        return withSystemDb(tx -> {
            Node node = tx.findNode(SystemLabels.ApocTriggerMeta, SystemPropertyKeys.database.name(), db.databaseName());
            return node == null ? 0L : (long) node.getProperty(SystemPropertyKeys.lastUpdated.name());
        });
    }
    
    private void updateCache() {
        activeTriggers.clear();

        lastUpdate = System.currentTimeMillis();

        withSystemDb(tx -> {
            tx.findNodes(SystemLabels.ApocTrigger,
                    SystemPropertyKeys.database.name(), db.databaseName()).forEachRemaining(
                    node -> activeTriggers.put(
                            (String) node.getProperty(SystemPropertyKeys.name.name()),
                            MapUtil.map(
                                    "statement", node.getProperty(SystemPropertyKeys.statement.name()),
                                    "selector", Util.fromJson((String) node.getProperty(SystemPropertyKeys.selector.name()), Map.class),
                                    "params", Util.fromJson((String) node.getProperty(SystemPropertyKeys.params.name()), Map.class),
                                    "paused", node.getProperty(SystemPropertyKeys.paused.name())
                            )
                    )
            );
            return null;
        });

        reconcileKernelRegistration();
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

}
