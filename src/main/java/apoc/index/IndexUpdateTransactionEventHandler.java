package apoc.index;

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
import org.neo4j.logging.Log;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

/**
 * a transaction event handler that updates manual indexes based on configuration in graph properties
 * based on configuration the updates are process synchronously via {@link #beforeCommit(TransactionData)} or async via
 * {@link #afterCommit(TransactionData, Collection<Consumer<Void>)}
 * @author Stefan Armbruster
 */
public class IndexUpdateTransactionEventHandler extends TransactionEventHandler.Adapter<Collection<Consumer<Void>>> {

    private final GraphDatabaseService graphDatabaseService;
    private final Log log;
    private final boolean async;

    private final BlockingQueue<Consumer<Void>> indexCommandQueue = new LinkedBlockingQueue<>(100);
    private Map<String, Map<String, Collection<Index<Node>>>> indexesByLabelAndProperty;

    public IndexUpdateTransactionEventHandler(GraphDatabaseAPI graphDatabaseService, Log log, boolean async) {
        this.graphDatabaseService = graphDatabaseService;
        this.log = log;
        this.async = async;
        Pools.SCHEDULED.scheduleAtFixedRate(() -> initIndexConfiguration(),10,10, TimeUnit.SECONDS);
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

        iterateNodePropertyChange(data.assignedNodeProperties(), (index, node, key, value, oldValue) -> indexUpdate(state, aVoid -> {
            if (oldValue != null) {
                index.remove(node, key);
                index.remove(node, FreeTextSearch.KEY);
            }
            index.add(node, key, value);
            index.add(node, FreeTextSearch.KEY, value);
        }));

        iterateNodePropertyChange(data.removedNodeProperties(), (index, node, key, value, oldValue) -> indexUpdate(state, aVoid -> {
            index.remove(node, key);
            index.remove(node, FreeTextSearch.KEY);
        }));

        iterateLabelChanges(data.assignedLabels(), data.createdNodes(), (index, node, key, value, ignore) -> indexUpdate(state, aVoid -> {
            index.add(node, key, value);
            index.add(node, FreeTextSearch.KEY, value);
        }));

        iterateLabelChanges(data.removedLabels(), data.deletedNodes(), (index, node, key, value, ignore) -> indexUpdate(state, aVoid -> {
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

    private void iterateNodePropertyChange(Iterable<PropertyEntry<Node>> propertyEntries, IndexFunction<Index<Node>, Node, String, Object, Object> function) {
        propertyEntries.forEach(nodePropertyEntry -> {
            final Node entity = nodePropertyEntry.entity();
            final String key = nodePropertyEntry.key();
            final Object value = nodePropertyEntry.value();
            Collection<Index<Node>> indices = findIndicesAffectedBy(entity.getLabels(), key);
            for (Index<Node> index : indices) {
                function.apply(index, entity, key, value, nodePropertyEntry.previouslyCommitedValue());
            }
        });
    }

    private void iterateLabelChanges(Iterable<LabelEntry> labelEntries, Iterable<Node> blacklist, IndexFunction<Index<Node>, Node, String, Object, Void> function) {
        StreamSupport.stream(labelEntries.spliterator(), false)
                .filter(labelEntry -> iterableContains(blacklist, labelEntry.node()))
                .forEach(labelEntry -> {
                    final Map<String, Collection<Index<Node>>> propertyIndicesMap = indexesByLabelAndProperty.get(labelEntry.label().name());
                    if (propertyIndicesMap!=null) {
                        final Node entity = labelEntry.node();
                        for (String key : entity.getPropertyKeys()) {
                            Collection<Index<Node>> indices = propertyIndicesMap.get(key);
                            if (indices != null) {
                                for (Index<Node> index : indices) {
                                    Object value = entity.getProperty(key);
                                    function.apply(index, entity, key, value, null);
                                }
                            }
                        }
                    }
                });
    }

    private boolean iterableContains(Iterable<Node> nodes, Node node) {
        for (Node thisNode: nodes) {
            if (thisNode.equals(node)) {
                return false;
            }
        }
        return true;
    }

    private Collection<Index<Node>> findIndicesAffectedBy(Iterable<Label> labels, String key) {
        Collection<Index<Node>> result = new ArrayList<>();
        for (Label label: labels) {
            Map<String, Collection<Index<Node>>> propertyIndexMap = indexesByLabelAndProperty.get(label.name());
            if (propertyIndexMap!=null) {
                final Collection<Index<Node>> indices = propertyIndexMap.get(key);
                if (indices!=null)  {
                    result.addAll(indices);
                }
            }
        }
        return result;
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
            initIndexConfiguration();
        }
        return indexesByLabelAndProperty;
    }

    // might be run from a scheduler, so we need to make sure we have a transaction
    private synchronized void initIndexConfiguration() {
        try (Transaction tx = graphDatabaseService.beginTx() ) {
            indexesByLabelAndProperty = new HashMap<>();

            final IndexManager indexManager = graphDatabaseService.index();
            for (String indexName : indexManager.nodeIndexNames()) {
                final Index<Node> index = indexManager.forNodes(indexName);
                Map<String, String> indexConfig = indexManager.getConfiguration(index);

                if (Util.toBoolean(indexConfig.get("autoUpdate"))) {
                    String labels = indexConfig.getOrDefault("labels", "");
                    for (String label : labels.split(":")) {
                        Map<String, Collection<Index<Node>>> propertyKeyToIndexMap = indexesByLabelAndProperty.computeIfAbsent(label, s -> new HashMap<>());
                        for (String property : indexConfig.getOrDefault("keysForLabel:" + label, "").split(":")) {
                            final Collection<Index<Node>> indices = propertyKeyToIndexMap.computeIfAbsent(property, s -> new ArrayList<>());
                            indices.add(index);
                        }
                    }
                }
            }
            tx.success();
        }
    }

}
