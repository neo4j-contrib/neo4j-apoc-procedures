package apoc.index;

import apoc.Description;
import apoc.result.WeightedNodeResult;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.index.impl.lucene.legacy.LuceneDataSource;
import org.neo4j.index.impl.lucene.legacy.LuceneIndexImplementation;
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
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toMap;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterables.single;

public class FreeTextSearch {
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
    public Stream<FulltextIndex.IndexInfo> addAllNodes(@Name("index") String index, @Name("structure") Map<String, List<String>> structure) {
        switch (structure.size()) {
            case 0:
                throw new IllegalArgumentException("No structure given.");
            case 1:
                return create(new Indexer.Single(index, db, log, structure));
            default:
                return create(new Indexer(index, db, log, structure));
        }
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
        return result(db.index().forNodes(index).query(parseFreeTextQuery(query)));
    }

    @Context
    public GraphDatabaseAPI db;

    @Context
    public Log log;

    // BEGIN: implementation

    static Stream<WeightedNodeResult> result(IndexHits<Node> hits) {
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

    private static final Map<String, String> CONFIG = LuceneIndexImplementation.FULLTEXT_CONFIG;
    static final String KEY = "search";
    private static final JobScheduler.Group GROUP = new JobScheduler.Group(
            FreeTextSearch.class.getSimpleName(), JobScheduler.SchedulingStrategy.POOLED);

    private Stream<FulltextIndex.IndexInfo> create(Indexer indexer) {
        return async(executor(), "Creating index '" + indexer.name + "'", indexer);
    }

    private Executor executor() {
        return db.getDependencyResolver().resolveDependency(JobScheduler.class).executor(GROUP);
    }

    static Analyzer analyzer() {
        return LuceneDataSource.LOWER_CASE_WHITESPACE_ANALYZER;
    }

    private static class Indexer implements Consumer<Consumer<FulltextIndex.IndexInfo>> {
        private final String name;
        final GraphDatabaseService db;
        private final Log log;
        final Set<String> labels = new HashSet<>();
        final Map<Set<String/*labels*/>, Properties> properties = new HashMap<>();

        Indexer(String name, GraphDatabaseService db, Log log, Map<String, List<String>> structure) {
            this.name = name;
            this.db = db;
            this.log = log;
            for (Map.Entry<String, List<String>> entry : structure.entrySet()) {
                labels.add(entry.getKey());
                properties.put(singleton(entry.getKey()),
                        new Properties(entry.getKey(), new HashSet<>(entry.getValue())));
            }
        }

        ResourceIterator<Node> nodes() {
            return db.getAllNodes().iterator();
        }

        Properties properties(Node node) {
            Set<String> labels = labels(node);
            if (labels == null) {
                return null;
            }
            Properties properties = this.properties.get(labels);
            if (properties == null) {
                properties = new Properties(labels.stream().map(label -> this.properties.get(singleton(label))));
                this.properties.put(labels, properties);
            }
            return properties;
        }

        private Set<String> labels(Node node) {
            Set<String> labels = null;
            for (Label label : node.getLabels()) {
                String name = label.name();
                if (this.labels.contains(name)) {
                    if (labels == null) {
                        labels = singleton(name);
                        continue;
                    } else if (labels.size() == 1) {
                        labels = new HashSet<>(labels);
                    }
                    labels.add(name);
                }
            }
            return labels;
        }

        @Override
        public final void accept(Consumer<FulltextIndex.IndexInfo> result) {
            Transaction tx = db.beginTx();
            try {
                int batch = 0;
                if (db.index().existsForNodes(name)) {
                    Index<Node> old = db.index().forNodes(name);
                    Map<String, String> config = db.index().getConfiguration(old);
                    log.info("Dropping existing index '%s', with config: %s", name, config);
                    old.delete();
                }

                Index<Node> index = db.index().forNodes(name, configuration());

                try (ResourceIterator<Node> nodes = nodes()) {
                    for (int count = 0; nodes.hasNext(); ) {
                        Node node = nodes.next();

                        Properties properties = properties(node);
                        if (properties != null) {
                            properties.index(node, index);
                            count++;
                        }

                        if (count == 50_000) {
                            tx.success();
                            tx.close();
                            tx = db.beginTx();
                            count = 0;
                            log.info("Committing batch %s in creation of index '%s'", ++batch, name);
                        }
                    }
                }

                log.info("Committing batch %s in creation of index '%s' (last batch)", ++batch, name);

                result.accept(new FulltextIndex.IndexInfo(FulltextIndex.NODE, name, CONFIG));

                tx.success();
            } finally {
                tx.close();
            }
        }

        private Map<String, String> configuration() {
            Map<String, String> config = new HashMap<>(CONFIG);
            config.put("labels", escape(labels));
            for (String label : labels) {
                Properties properties = this.properties.get(singleton(label));
                config.put("keysForLabel:" + escape(label), escape(asList(properties.keys)));
            }
            return config;
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

        private static class Single extends Indexer {
            private final Label label;
            private final Properties single;

            Single(String name, GraphDatabaseService db, Log log, Map<String, List<String>> structure) {
                super(name, db, log, structure);
                this.label = label(single(this.labels));
                this.single = single(this.properties.values());
            }

            @Override
            ResourceIterator<Node> nodes() {
                return db.findNodes(label);
            }

            @Override
            Properties properties(Node node) {
                return node.hasLabel(label) ? single : null;
            }
        }
    }

    private static class Properties {
        private final String[] keys;
        private final Map<String/*key*/, List<String/*fields*/>> fields;

        Properties(String label, Collection<String> properties) {
            this.keys = properties.toArray(new String[properties.size()]);
            this.fields = properties.stream().collect(toMap(key -> key, key -> singletonList(label + "." + key)));
        }

        Properties(Stream<Properties> properties) {
            Set<String> keys = new HashSet<>();
            this.fields = new HashMap<>();
            properties.forEach(props -> {
                addAll(keys, props.keys);
                for (Map.Entry<String, List<String>> entry : props.fields.entrySet()) {
                    fields.compute(entry.getKey(), (k, v) -> {
                        if (v == null) {
                            return entry.getValue();
                        } else {
                            List<String> value = new ArrayList<>(v.size() + entry.getValue().size());
                            value.addAll(v);
                            value.addAll(entry.getValue());
                            return value;
                        }
                    });
                }
            });
            this.keys = keys.toArray(new String[keys.size()]);
        }

        <T extends PropertyContainer> void index(T entity, Index<T> index) {
            for (Map.Entry<String, Object> property : entity.getProperties(keys).entrySet()) {
                Object value = property.getValue();
                index.add(entity, KEY, value.toString());
                for (String field : fields.get(property.getKey())) {
                    index.add(entity, field, value
                    );
                }
            }
        }
    }
}
