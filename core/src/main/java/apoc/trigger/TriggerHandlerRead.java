package apoc.trigger;

import apoc.Pools;
import apoc.SystemPropertyKeys;
import apoc.util.Util;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static apoc.trigger.TriggerHandlerWrite.getTriggerNodes;
import static apoc.trigger.TriggerHandlerWrite.withSystemDb;


public class TriggerHandlerRead extends LifecycleAdapter implements TransactionEventListener<Void> {

    private enum Phase {before, after, rollback, afterAsync}

    private final Log log;
    private final GraphDatabaseService db;
    private final DatabaseManagementService databaseManagementService;
    private final Pools pools;

    private final AtomicBoolean registeredWithKernel = new AtomicBoolean(false);

    public TriggerHandlerRead(GraphDatabaseService db, DatabaseManagementService databaseManagementService,
                              Log log, Pools pools) {
        this.db = db;
        this.databaseManagementService = databaseManagementService;
        this.log = log;
        this.pools = pools;
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
        return getTriggers();
    }

    private Map<String, Map<String, Object>> getTriggers() {
        // TODO - in `dev` change StreamSupport.stream with Iterators.stream 
        //      depends on https://github.com/neo4j/apoc/pull/187/files
        return withSystemDb(
                tx -> StreamSupport.stream(
                        getTriggerNodes(db.databaseName(), tx).stream().spliterator(), 
                                false)
                        .collect(Collectors.toMap(
                                node -> (String) node.getProperty( SystemPropertyKeys.name.name() ),
                                        TriggerHandlerWrite::toTriggerInfo)
                        )
        );
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
        return getTriggers().values().stream()
                .map(data -> (Map<String, Object>) data.get("selector"))
                .anyMatch(selector -> when(selector, phase));
    }

    private void executeTriggers(Transaction tx, TransactionData txData, Phase phase) {
        executeTriggers(tx, TriggerMetadata.from(txData, false), phase);
    }

    private void executeTriggers(Transaction tx, TriggerMetadata triggerMetadata, Phase phase) {
        Map<String,String> exceptions = new LinkedHashMap<>();
        getTriggers().forEach((name, data) -> {
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
        reconcileKernelRegistration();
    }

    @Override
    public void stop() {
        if(registeredWithKernel.compareAndSet(true, false)) {
            databaseManagementService.unregisterTransactionEventListener(db.databaseName(), this);
        }
    }

}
