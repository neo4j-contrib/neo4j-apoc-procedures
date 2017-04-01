package apoc.index;

import apoc.ApocConfiguration;
import apoc.Pools;
import apoc.util.Util;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.neo4j.helpers.collection.Iterables.stream;
import static org.neo4j.helpers.collection.Iterators.*;

/**
 * a transaction event handler that updates manual indexes based on configuration in graph properties
 * based on configuration the updates are process synchronously via {@link #beforeCommit(TransactionData)} or async via
 * {@link #afterCommit(TransactionData, Collection<Consumer<Void>)}
 * @author Stefan Armbruster
 */
public class IndexUpdateTransactionEventHandler extends TransactionEventHandler.Adapter<Collection<Consumer<Void>>> {

    private final GraphDatabaseService graphDatabaseService;
    private final boolean async;

    private final BlockingQueue<Consumer<Void>> indexCommandQueue = new LinkedBlockingQueue<>(100);
    private Map<String, Map<String, Collection<Index<Node>>>> indexesByLabelAndProperty;

    public IndexUpdateTransactionEventHandler(GraphDatabaseAPI graphDatabaseService, boolean async) {
        this.graphDatabaseService = graphDatabaseService;
        this.async = async;
        Pools.SCHEDULED.scheduleAtFixedRate(() -> indexesByLabelAndProperty = initIndexConfiguration(),10,10, TimeUnit.SECONDS);
    }

    public BlockingQueue<Consumer<Void>> getIndexCommandQueue() {
        return indexCommandQueue;
    }

    @FunctionalInterface
    interface IndexFunction<A, B, C, D, E> {
        void apply (A a, B b, C c, D d, E e);
    }

    @Override
    public Collection<Consumer<Void>> beforeCommit(TransactionData data) throws Exception {
        getIndexesByLabelAndProperty();
        Collection<Consumer<Void>> state = async ? new LinkedList<>() : null;

        iterateNodePropertyChange(stream(data.assignedNodeProperties()),(index, node, key, value, oldValue) -> indexUpdate(state, aVoid -> {
            if (oldValue != null) {
                index.remove(node, key);
                index.remove(node, FreeTextSearch.KEY);
            }
            index.add(node, key, value);
            index.add(node, FreeTextSearch.KEY, value);
        }));

        // filter out removedNodeProperties from node deletions
        iterateNodePropertyChange(stream(data.removedNodeProperties()).filter(nodePropertyEntry -> !contains(data.deletedNodes().iterator(), nodePropertyEntry.entity())), (index, node, key, value, oldValue) -> indexUpdate(state, aVoid -> {
            index.remove(node, key);
            index.remove(node, FreeTextSearch.KEY);
        }));

        iterateLabelChanges(stream(data.assignedLabels()).filter(labelEntry -> !contains(data.createdNodes().iterator(), labelEntry.node())), (index, node, key, value, ignore) -> indexUpdate(state, aVoid -> {
            index.add(node, key, value);
            index.add(node, FreeTextSearch.KEY, value);
        }));

        iterateLabelChanges(stream(data.removedLabels()).filter(labelEntry -> !contains(data.deletedNodes().iterator(), labelEntry.node())), (index, node, key, value, ignore) -> indexUpdate(state, aVoid -> {
            index.remove(node, key);
            index.remove(node, FreeTextSearch.KEY);
        }));

        return state;
    }

    @Override
    public void afterCommit(TransactionData data, Collection<Consumer<Void>> state) {
        if (async) {
            for (Consumer<Void> consumer: state) {
                try {
                    indexCommandQueue.put(consumer);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void iterateNodePropertyChange(Stream<PropertyEntry<Node>> stream,
          IndexFunction<Index<Node>, Node, String, Object, Object> function) {
        stream.forEach(nodePropertyEntry -> {
            final Node entity = nodePropertyEntry.entity();
            final String key = nodePropertyEntry.key();
            final Object value = nodePropertyEntry.value();
            entity.getLabels().forEach(label -> {
                final String labelName = label.name();
                final Map<String, Collection<Index<Node>>> propertyIndexMap = indexesByLabelAndProperty.get(labelName);
                if (propertyIndexMap!=null) {
                    final Collection<Index<Node>> indices = propertyIndexMap.get(key);
                    if (indices!= null) {
                        for (Index<Node> index : indices) {
                            String indexKey = labelName + "." + key;
                            function.apply(index, entity, indexKey, value, nodePropertyEntry.previouslyCommitedValue());
                        }
                    }
                }
            });
        });
    }

    private void iterateLabelChanges(Stream<LabelEntry> stream, IndexFunction<Index<Node>, Node, String, Object, Void> function) {
        stream.forEach(labelEntry -> {
            final String labelName = labelEntry.label().name();
            final Map<String, Collection<Index<Node>>> propertyIndicesMap = indexesByLabelAndProperty.get(labelName);
            if (propertyIndicesMap != null) {
                final Node entity = labelEntry.node();
                for (String key : entity.getPropertyKeys()) {
                    Collection<Index<Node>> indices = propertyIndicesMap.get(key);
                    if (indices != null) {
                        for (Index<Node> index : indices) {
                            Object value = entity.getProperty(key);
                            String indexKey = labelName + "." + key;
                            function.apply(index, entity, indexKey, value, null);
                        }
                    }
                }
            }
        });
    }

    /**
     * in async mode add the index action to a collection for consumption in {@link #afterCommit(TransactionData, Collection)}, in sync mode, run it directly
     */
    private Void indexUpdate(Collection<Consumer<Void>> state, Consumer<Void> indexAction) {
        if (state==null) {  // sync
            indexAction.accept(null);
        } else { // async
            state.add(indexAction);
        }
        return null;
    }

    public Map<String, Map<String, Collection<Index<Node>>>> getIndexesByLabelAndProperty() {
        if (indexesByLabelAndProperty == null ) {
            indexesByLabelAndProperty = initIndexConfiguration();
        }
        return indexesByLabelAndProperty;
    }

    public void resetConfiguration() {
        indexesByLabelAndProperty = null;
    }

    // might be run from a scheduler, so we need to make sure we have a transaction
    private synchronized Map<String, Map<String, Collection<Index<Node>>>> initIndexConfiguration() {
        Map<String, Map<String, Collection<Index<Node>>>> indexesByLabelAndProperty = new HashMap<>();
        try (Transaction tx = graphDatabaseService.beginTx() ) {

            final IndexManager indexManager = graphDatabaseService.index();
            for (String indexName : indexManager.nodeIndexNames()) {
                final Index<Node> index = indexManager.forNodes(indexName);
                Map<String, String> indexConfig = indexManager.getConfiguration(index);

                if (Util.toBoolean(indexConfig.get("autoUpdate"))) {
                    String labels = indexConfig.getOrDefault("labels", "");
                    for (String label : labels.split(":")) {
                        Map<String, Collection<Index<Node>>> propertyKeyToIndexMap = indexesByLabelAndProperty.computeIfAbsent(label, s -> new HashMap<>());
                        String[] keysForLabel = indexConfig.getOrDefault("keysForLabel:" + label, "").split(":");
                        for (String property : keysForLabel) {
                            propertyKeyToIndexMap.computeIfAbsent(property, s -> new ArrayList<>()).add(index);
                        }
                    }
                }
            }
            tx.success();
        }
        return indexesByLabelAndProperty;
    }

    public static class LifeCycle {
        private final GraphDatabaseAPI db;
        private final Log log;
        private IndexUpdateTransactionEventHandler indexUpdateTransactionEventHandler;

        public LifeCycle(GraphDatabaseAPI db, Log log) {
            this.db = db;
            this.log = log;
        }

        public void start() {
            boolean enabled = ApocConfiguration.isEnabled("autoIndex.enabled");
            if (enabled) {
                boolean async = ApocConfiguration.isEnabled("autoIndex.async");
                indexUpdateTransactionEventHandler = new IndexUpdateTransactionEventHandler(db, async);
                if (async) {
                    startIndexTrackingThread(db, indexUpdateTransactionEventHandler.getIndexCommandQueue(),
                            Long.parseLong(ApocConfiguration.get("autoIndex.async_rollover_opscount", "10000")),
                            Long.parseLong(ApocConfiguration.get("autoIndex.async_rollover_millis", "5000")),
                            log
                    );
                }
                db.registerTransactionEventHandler(indexUpdateTransactionEventHandler);
            }
        }
        private void startIndexTrackingThread(GraphDatabaseAPI db, BlockingQueue<Consumer<Void>> indexCommandQueue, long opsCountRollover, long millisRollover, Log log) {
            new Thread(() -> {
                Transaction tx = db.beginTx();
                int opsCount = 0;
                long lastCommit = System.currentTimeMillis();
                try {
                    while (true) {
                        Consumer<Void> indexCommand = indexCommandQueue.poll(millisRollover, TimeUnit.MILLISECONDS);
                        long now = System.currentTimeMillis();
                        if ((opsCount>0) && ((now - lastCommit > millisRollover) || (opsCount > opsCountRollover))) {
                            tx.success();
                            tx.close();
                            tx = db.beginTx();
                            lastCommit = now;
                            opsCount = 0;
                            log.info("background indexing thread doing tx rollover");
                        }
                        if (indexCommand == null) {
                            // check if a database shutdown is already in progress, if so, terminate this thread
                            boolean running = db.getDependencyResolver().resolveDependency(LifeSupport.class).isRunning();
                            if (!running) {
                                log.info("system shutdown detected, terminating indexing background thread");
                                break;
                            }
                        } else {
                            opsCount++;
                            indexCommand.accept(null);
                        }
                    }
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                    throw new RuntimeException(e);
                } finally {
                    tx.success();
                    tx.close();
                    log.info("stopping background thread for async index updates");
                }
            }).start();
            log.info("started background thread for async index updates");
        }

        public void stop() {
            if (indexUpdateTransactionEventHandler!=null) {
                db.unregisterTransactionEventHandler(indexUpdateTransactionEventHandler);
            }
        }

        public void resetConfiguration() {
            if (indexUpdateTransactionEventHandler!=null) {
                indexUpdateTransactionEventHandler.resetConfiguration();
            }
        }
    }
}
