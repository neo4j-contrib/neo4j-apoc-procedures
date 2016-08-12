package apoc.cypher;

import apoc.Description;
import apoc.Pools;
import apoc.result.MapResult;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.util.MapUtil.map;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

/**
 * @author mh
 * @since 08.05.16
 */
public class Cypher {

    public static final String COMPILED_PREFIX = ""; //  "CYPHER runtime=compiled ";
    public static final ExecutorService POOL = Pools.DEFAULT;
    public static final int PARTITIONS = 100 * Runtime.getRuntime().availableProcessors();
    public static final int MAX_BATCH = 10000;
    @Context
    public GraphDatabaseService db;
    @Context
    public GraphDatabaseAPI api;
    @Context
    public KernelTransaction tx;
    @Context
    public Log log;

    @Procedure
    @Description("apoc.cypher.run(fragment, params) yield value - executes reading fragment with the given parameters")
    public Stream<MapResult> run(@Name("cypher") String statement, @Name("params") Map<String, Object> params) {
        if (params == null) params = Collections.emptyMap();
        return db.execute(compiled(statement, params.keySet()), params).stream().map(MapResult::new);
    }

    @Procedure
    @Description("apoc.cypher.runFile(file or url) - runs each statement in the file, all semicolon separated - currently no schema operations")
    public Stream<RowResult> runFile(@Name("file") String fileName) {
        Reader reader = readerForFile(fileName);
        Scanner scanner = new Scanner(reader);
        return runManyStatements(scanner, Collections.emptyMap());
    }

    private Stream<RowResult> runManyStatements(Scanner scanner, Map<String, Object> params) {
        scanner.useDelimiter(";\r?\n");
        BlockingQueue<RowResult> queue = new ArrayBlockingQueue<>(100);
        while (scanner.hasNext()) {
            String stmt = scanner.next();
            if (isSchemaOperation(stmt)) // alternatively could just skip them
                throw new RuntimeException("Schema Operations can't yet be mixed with data operations");

            if (isPeriodicOperation(stmt)) Util.inThread(() -> executeStatement(queue, stmt, params));
            else Util.inTx(api, () -> executeStatement(queue, stmt, params));
        }
        Util.inThread(() -> { queue.put(RowResult.TOMBSTONE);return null;});
        return StreamSupport.stream(new QueueBasedSpliterator<>(queue, RowResult.TOMBSTONE),false);
    }

    @Procedure
    @Description("apoc.cypher.runMany('cypher;\\nstatements;',{params}) - runs each semicolon separated statement and returns summary - currently no schema operations")
    public Stream<RowResult> runMany(@Name("cypher") String cypher, @Name("params") Map<String,Object> params) {
        return runManyStatements(new Scanner(cypher),params);
    }

    private Object executeStatement(BlockingQueue<RowResult> queue, String stmt, Map<String, Object> params) throws InterruptedException {
        try (Result result = api.execute(stmt,params)) {
            long time = System.currentTimeMillis();
            int row = 0;
            while (result.hasNext()) {
                if (tx.getReasonIfTerminated()!=null) break;
                queue.put(new RowResult(row++, result.next()));
            }
            queue.put(new RowResult(-1, toMap(result.getQueryStatistics(), System.currentTimeMillis() - time, row)));
            return null;
        }
    }

    private boolean isSchemaOperation(String stmt) {
        return stmt.matches("(?is).*(create|drop)\\s+(index|constraint).*");
    }
    private boolean isPeriodicOperation(String stmt) {
        return stmt.matches("(?is).*using\\s+periodic.*");
    }

    private Map<String, Object> toMap(QueryStatistics stats, long time, long rows) {
        return map(
                "rows",rows,
                "time",time,
                "nodesCreated",stats.getNodesCreated(),
                "nodesDeleted",stats.getNodesDeleted(),
                "labelsAdded",stats.getLabelsAdded(),
                "labelsRemoved",stats.getLabelsRemoved(),
                "relationshipsCreated",stats.getRelationshipsCreated(),
                "relationshipsDeleted",stats.getRelationshipsDeleted(),
                "propertiesSet",stats.getPropertiesSet(),
                "constraintsAdded",stats.getConstraintsAdded(),
                "constraintsRemoved",stats.getConstraintsRemoved(),
                "indexesAdded",stats.getIndexesAdded(),
                "indexesRemoved",stats.getIndexesRemoved()
        );
    }

    public static class RowResult {
        public static final RowResult TOMBSTONE = new RowResult(-1,null);
        public long row;
        public Map<String,Object> result;

        public RowResult(long row, Map<String, Object> result) {
            this.row = row;
            this.result = result;
        }
    }
    private Reader readerForFile(@Name("file") String fileName) {
        try {
            try {
                return new BufferedReader(new InputStreamReader(new URL(fileName).openStream(), "UTF-8"));
            } catch (MalformedURLException mue) {
                return new BufferedReader(new FileReader(new File(fileName)));
            }
        } catch (IOException ioe) {
            throw new RuntimeException("Error accessing file "+fileName,ioe);
        }
    }

    public static String compiled(String fragment, Collection<String> keys) {
        if (keys.isEmpty()) return fragment;
        String declaration = " WITH " + join(", ", keys.stream().map(s -> format(" {`%s`} as `%s` ", s, s)).collect(toList()));
        return declaration + fragment;
//        return fragment.substring(0,6).equalsIgnoreCase("cypher") ? fragment : COMPILED_PREFIX + fragment;
    }

    @Procedure
    public Stream<MapResult> parallel(@Name("fragment") String fragment, @Name("params") Map<String, Object> params, @Name("parallelizeOn") String key) {
        if (params == null) return run(fragment, params);
        if (key == null || !params.containsKey(key))
            throw new RuntimeException("Can't parallelize on key " + key + " available keys " + params.keySet());
        Object value = params.get(key);
        if (!(value instanceof Collection))
            throw new RuntimeException("Can't parallelize a non collection " + key + " : " + value);

        final String statement = compiled(fragment, params.keySet());
        Collection<Object> coll = (Collection<Object>) value;
        return coll.parallelStream().flatMap((v) -> {
            if (tx.getReasonIfTerminated()!=null) return Stream.of(MapResult.empty());
            Map<String, Object> parallelParams = new HashMap<>(params);
            parallelParams.replace(key, v);
            return api.execute(statement, parallelParams).stream().map(MapResult::new);
        });

        /*
        params.entrySet().stream()
                .filter( e -> asCollection(e.getValue()).size() > 100)
                .map( (e) -> (Map.Entry<String,Collection>)(Map.Entry)e )
                .max( (max,e) -> e.getValue().size() )
                .map( (e) -> e.getValue().parallelStream().map( (v) -> {
                    Map map = new HashMap<>(params);
                    map.put(e.getKey(),as)
                }));
        return db.execute(statement,params).stream().map(MapResult::new);
        */
    }


    @Procedure
    @Description("apoc.cypher.mapParallel(fragment, params, list-to-parallelize) yield value - executes fragment in parallel batches with the list segments being assigned to _")
    public Stream<MapResult> mapParallel(@Name("fragment") String fragment, @Name("params") Map<String, Object> params, @Name("list") List<Object> data) {
        final String statement = parallelStatement(fragment, params, "_");
        db.execute("EXPLAIN " + statement).close();
        return Util.partitionSubList(data, PARTITIONS)
                .flatMap((partition) -> Iterators.addToCollection(api.execute(statement, parallelParams(params, "_", partition)),
                        new ArrayList<>(partition.size())).stream())
                .map(MapResult::new);
    }

    // todo proper Collector
    public Stream<List<Object>> partitionColl(@Name("list") Collection<Object> list, int partitions) {
        int total = list.size();
        int batchSize = Math.max(total / partitions, 1);
        List<List<Object>> result = new ArrayList<>(PARTITIONS);
        List<Object> partition = new ArrayList<>(batchSize);
        for (Object o : list) {
            partition.add(o);
            if (partition.size() < batchSize) continue;
            result.add(partition);
            partition = new ArrayList<>(batchSize);
        }
        if (!partition.isEmpty()) {
            result.add(partition);
        }
        return result.stream();
    }

    public Map<String, Object> parallelParams(@Name("params") Map<String, Object> params, String key, List<Object> partition) {
        if (params.isEmpty()) return Collections.singletonMap(key, partition);

        Map<String, Object> parallelParams = new HashMap<>(params);
        parallelParams.put(key, partition);
        return parallelParams;
    }

    @Procedure
    public Stream<MapResult> parallel2(@Name("fragment") String fragment, @Name("params") Map<String, Object> params, @Name("parallelizeOn") String key) {
        if (params == null) return run(fragment, params);
        if (key == null || !params.containsKey(key))
            throw new RuntimeException("Can't parallelize on key " + key + " available keys " + params.keySet());
        Object value = params.get(key);
        if (!(value instanceof Collection))
            throw new RuntimeException("Can't parallelize a non collection " + key + " : " + value);

        final String statement = parallelStatement(fragment, params, key);
        db.execute("EXPLAIN " + statement).close();
        Collection<Object> coll = (Collection<Object>) value;
        int total = coll.size();
        int partitions = PARTITIONS;
        int batchSize = Math.max(total / partitions, 1);
        if (batchSize > MAX_BATCH) {
            batchSize = MAX_BATCH;
            partitions = (total / batchSize) + 1;
        }

        List<Future<List<Map<String, Object>>>> futures = new ArrayList<>(partitions);
        List<Object> partition = new ArrayList<>(batchSize);
        for (Object o : coll) {
            partition.add(o);
            if (partition.size() == batchSize) {
                if (tx.getReasonIfTerminated()!=null) return Stream.of(MapResult.empty());
                futures.add(submit(api, statement, params, key, partition));
                partition = new ArrayList<>(batchSize);
            }
        }
        if (!partition.isEmpty()) {
            futures.add(submit(api, statement, params, key, partition));
        }
        return futures.stream().flatMap(f -> {
            try {
                return f.get().stream().map(MapResult::new);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Error executing in parallell " + statement, e);
            }
        });
    }

    public String parallelStatement(@Name("fragment") String fragment, @Name("params") Map<String, Object> params, @Name("parallelizeOn") String key) {
        StringBuilder sb = new StringBuilder(200);
        boolean first = true;
        for (String s : params.keySet()) {
            if (s.equals(key)) continue;
            if (first) {
                first = false;
                sb.append(" WITH ");
            } else {
                sb.append(", ");
            }
            sb.append(format(" {`%s`} as `%s` ", s, s));
        }
        sb.append("UNWIND {`").append(key).append("`} as `").append(key).append("` ");
        return sb.toString() + fragment;
    }

    private Future<List<Map<String, Object>>> submit(GraphDatabaseAPI api, String statement, Map<String, Object> params, String key, List<Object> partition) {
        return POOL.submit(() -> Iterators.addToCollection(api.execute(statement, parallelParams(params, key, partition)), new ArrayList<>(partition.size())));
    }

    private static Collection asCollection(Object value) {
        if (value instanceof Collection) return (Collection) value;
        if (value instanceof Iterable) return Iterables.asCollection((Iterable) value);
        if (value instanceof Iterator) return Iterators.asCollection((Iterator) value);
        return Collections.singleton(value);
    }

    @Procedure
    @PerformsWrites
    @Description("apoc.cypher.doIt(fragment, params) yield value - executes writing fragment with the given parameters")
    public Stream<MapResult> doit(@Name("cypher") String statement, @Name("params") Map<String, Object> params) {
        if (params == null) params = Collections.emptyMap();
        return db.execute(compiled(statement, params.keySet()), params).stream().map(MapResult::new);
    }

    private class QueueBasedSpliterator<T> implements Spliterator<T> {
        private final BlockingQueue<T> queue;
        private T tombstone;

        public QueueBasedSpliterator(BlockingQueue<T> queue, T tombstone) {
            this.queue = queue;
            this.tombstone = tombstone;
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            try {
                if (tx.getReasonIfTerminated()!=null) return false;
                T entry = queue.poll(100, MILLISECONDS);
                if (entry == null || entry == tombstone) return false;
                action.accept(entry);
                return true;
            } catch (InterruptedException e) {
                return false;
            }
        }

        public Spliterator<T> trySplit() { return null; }

        public long estimateSize() { return Long.MAX_VALUE; }

        public int characteristics() { return ORDERED | NONNULL; }
    }

    @Procedure
    @Description("apoc.cypher.runTimeboxed('cypherStatement',{params}, timeout) - abort statement after timeout ms if not finished")
    public Stream<MapResult> runTimeboxed(@Name("cypher") String cypher, @Name("params") Map<String, Object> params, @Name("timeout") long timeout) {

        Pools.SCHEDULED.schedule(() -> {
            String txString = tx == null ? "<null>" : tx.toString();
            log.warn("marking " + txString + " for termination");
            tx.markForTermination(Status.Transaction.Terminated);
        }, timeout, MILLISECONDS);

        Result result = db.execute(cypher, params == null ? Collections.EMPTY_MAP : params);
        return result.stream().map(stringObjectMap -> new MapResult(stringObjectMap));
    }
}
