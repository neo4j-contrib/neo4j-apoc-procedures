package apoc.index;

import apoc.Description;
import apoc.result.WeightedNodeResult;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Sort;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.index.impl.lucene.legacy.LuceneDataSource;
import org.neo4j.index.impl.lucene.legacy.LuceneIndexImplementation;
import org.neo4j.index.lucene.QueryContext;
import org.neo4j.index.lucene.ValueContext;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.index.FreeTextQueryParser.parseFreeTextQuery;
import static apoc.util.AsyncStream.async;

public class FreeTextSearch {
    public static class IndexStats {
        public final String label;
        public final String property;
        public final long nodeCount;

        private IndexStats(String label, String property, long nodeCount) {
            this.label = label;
            this.property = property;
            this.nodeCount = nodeCount;
        }
    }

    /**
     * Create (or recreate) a free text search index.
     * <p>
     * This will populate the index with all currently matching data. Updates will not be reflected in the index.
     * In order to get updates into the index, the index has to be rebuilt.
     *
     * @param index     The name of the index to create.
     * @param structure The labels of nodes to index, and the properties to index for each label.
     * @return a stream containing a single element that describes the created index.
     */
    @Procedure
    @PerformsWrites
    @Description("apoc.index.addAllNodes('name',{label1:['prop1',...],...}) YIELD type, name, config - create a free text search index")
    public Stream<IndexStats> addAllNodes(@Name("index") String index, @Name("structure") Map<String, List<String>> structure) {
        if (structure.isEmpty()) {
            throw new IllegalArgumentException("No structure given.");
        }
        return async(executor(), "Creating index '" + index + "'", result -> {
            populate(index(index, structure), structure, result);
        });
    }

    /**
     * Search in the specified index for nodes matching the the given value.
     * <p>
     * Any indexed property is searched.
     *
     * @param index The name of the index to search in.
     * @param query The query specifying what to search for.
     * @return a stream of all matching nodes.
     */
    @Procedure
    @PerformsWrites
    @Description("apoc.index.search('name', 'query') YIELD node, weight - search for nodes in the free text index matching the given query")
    public Stream<WeightedNodeResult> search(@Name("index") String index, @Name("query") String query) throws ParseException {
        if (!db.index().existsForNodes(index)) {
            return Stream.empty();
        }
        return result(db.index().forNodes(index).query(
                new QueryContext(parseFreeTextQuery(query)).sort(Sort.RELEVANCE).top(100)));
    }

    @Context
    public GraphDatabaseAPI db;

    @Context
    public Log log;

    // BEGIN: implementation

    private static final Map<String, String> CONFIG = LuceneIndexImplementation.FULLTEXT_CONFIG;
    static final String KEY = "search";
    private static final JobScheduler.Group GROUP = new JobScheduler.Group(
            FreeTextSearch.class.getSimpleName(), JobScheduler.SchedulingStrategy.POOLED);

    private static Stream<WeightedNodeResult> result(IndexHits<Node> hits) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<WeightedNodeResult>() {
            @Override
            public boolean hasNext() {
                return hits.hasNext();
            }

            @Override
            public WeightedNodeResult next() {
                Node node = hits.next();
                float weight = hits.currentScore();
                return new WeightedNodeResult(node, weight);
            }
        }, 0), false);
    }

    private void populate(Index<Node> index, Map<String, List<String>> config, Consumer<IndexStats> result) {
        Map<String, String[]> structure = convertStructure(config);
        Map<LabelProperty, Counter> stats = new HashMap<>();
        Transaction tx = db.beginTx();
        try {
            int batch = 0;
            for (Node node : db.getAllNodes()) {
                boolean indexed = false;
                for (Label label : node.getLabels()) {
                    String[] keys = structure.get(label.name());
                    if (keys == null) continue;
                    indexed = true;
                    Map<String, Object> properties = keys.length == 0 ? node.getAllProperties() : node.getProperties(keys);
                    for (Map.Entry<String, Object> entry : properties.entrySet()) {
                        Object value = entry.getValue();
                        index.add(node, KEY, value.toString());
                        if (value instanceof Number) {
                            value = ValueContext.numeric(((Number) value).doubleValue());
                        }
                        index.add(node, label.name() + "." + entry.getKey(), value);
                        stats.computeIfAbsent(new LabelProperty(label.name(), entry.getKey()), x -> new Counter()).count++;
                    }
                }
                if (indexed) {
                    if (++batch == 50_000) {
                        batch = 0;
                        tx.success();
                        tx.close();
                        tx = db.beginTx();
                    }
                }
            }
            tx.success();
        } finally {
            tx.close();
        }
        stats.forEach((key,counter) -> result.accept(key.stats(counter)));
    }

    private Map<String, String[]> convertStructure(Map<String, List<String>> config) {
        Map<String, String[]> structure = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> entry : config.entrySet()) {
            List<String> props = entry.getValue();
            structure.put(entry.getKey(), props.toArray(new String[props.size()]));
        }
        return structure;
    }

    private Index<Node> index(String index, Map<String, List<String>> structure) {
        Map<String, String> config = new HashMap<>(CONFIG);
        try (Transaction tx = db.beginTx()) {
            if (db.index().existsForNodes(index)) {
                Index<Node> old = db.index().forNodes(index);
                config = new HashMap<>(db.index().getConfiguration(old));
                log.info("Dropping existing index '%s', with config: %s", index, config);
                old.delete();
            }
            tx.success();
        }
        try (Transaction tx = db.beginTx()) {
            updateConfigFromParameters(config, structure);
            Index<Node> nodeIndex = db.index().forNodes(index, config);
            tx.success();
            return nodeIndex;
        }
    }

    private void updateConfigFromParameters(Map<String, String> config, Map<String, List<String>> structure) {
        Iterator<String> it = config.keySet().iterator();
        while (it.hasNext()) {
            if (it.next().startsWith("keysForLabel:")) {
                it.remove();
            }
        }

        config.put("labels", escape(structure.keySet()));
        for (Map.Entry<String, List<String>> entry : structure.entrySet()) {
            config.put("keysForLabel:" + escape(entry.getKey()), escape(entry.getValue()));
        }
    }

    private Executor executor() {
        return db.getDependencyResolver().resolveDependency(JobScheduler.class).executor(GROUP);
    }

    static Analyzer analyzer() {
        return LuceneDataSource.LOWER_CASE_WHITESPACE_ANALYZER;
    }

    private static String escape(Collection<String> keys) {
        StringBuilder result = new StringBuilder();
        for (String key : keys) {
            if (result.length() > 0) {
                result.append(":");
            }
            result.append(escape(key));
        }
        return result.toString();
    }

    private static String escape(String key) {
        return key.replace("$", "$$").replace(":", "$");
    }

    private static class LabelProperty {
        private final String label, property;

        LabelProperty(String label, String property) {
            this.label = label;
            this.property = property;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LabelProperty that = (LabelProperty) o;
            return Objects.equals(label, that.label) &&
                    Objects.equals(property, that.property);
        }

        @Override
        public int hashCode() {
            return Objects.hash(label, property);
        }

        IndexStats stats(Counter counter) {
            return new IndexStats(label, property, counter.count);
        }
    }

    private static class Counter {
        long count;
    }
}
