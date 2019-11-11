package apoc.cypher;

import apoc.Pools;
import apoc.result.MapResult;
import apoc.util.FileUtils;
import apoc.util.QueueBasedSpliterator;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.util.MapUtil.map;
import static apoc.util.Util.param;
import static apoc.util.Util.quote;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.neo4j.procedure.Mode.WRITE;

/**
 * @author mh
 * @since 08.05.16
 */
public class Cypher {

    public static final String COMPILED_PREFIX = "CYPHER runtime="+ Util.COMPILED;
    public static final ExecutorService POOL = Pools.DEFAULT;
    public static final int PARTITIONS = 100 * Runtime.getRuntime().availableProcessors();
    public static final int MAX_BATCH = 10000;
    @Context
    public GraphDatabaseService db;
    @Context
    public Log log;
    @Context
    public TerminationGuard terminationGuard;

    /*
    TODO: add in alpha06
    @Context
    ProcedureTransaction procedureTransaction;
     */

    @Procedure
    @Description("apoc.cypher.run(fragment, params) yield value - executes reading fragment with the given parameters")
    public Stream<MapResult> run(@Name("cypher") String statement, @Name("params") Map<String, Object> params) {
        if (params == null) params = Collections.emptyMap();
        return db.execute(withParamMapping(statement, params.keySet()), params).stream().map(MapResult::new);
    }

    @Procedure(mode = WRITE)
    @Description("apoc.cypher.runFile(file or url,[{statistics:true,timeout:10,parameters:{}}]) - runs each statement in the file, all semicolon separated - currently no schema operations")
    public Stream<RowResult> runFile(@Name("file") String fileName, @Name(value = "config",defaultValue = "{}") Map<String,Object> config) {
        return runFiles(singletonList(fileName),config);
    }

    @Procedure(mode = WRITE)
    @Description("apoc.cypher.runFiles([files or urls],[{statistics:true,timeout:10,parameters:{}}])) - runs each statement in the files, all semicolon separated")
    public Stream<RowResult> runFiles(@Name("file") List<String> fileNames, @Name(value = "config",defaultValue = "{}") Map<String,Object> config) {
        boolean addStatistics = Util.toBoolean(config.getOrDefault("statistics",true));
        int timeout = Util.toInteger(config.getOrDefault("timeout",10));
        List<RowResult> result = new ArrayList<>();
        @SuppressWarnings( "unchecked" )
        Map<String,Object> parameters = (Map<String,Object>)config.getOrDefault("parameters",Collections.emptyMap());
        for (String f : fileNames) {
            List<RowResult> rowResults = runManyStatements(readerForFile(f), parameters, false, addStatistics, timeout).collect(Collectors.toList());
            result.addAll(rowResults);
        }
        return result.stream();
    }

    @Procedure(mode=Mode.SCHEMA)
    @Description("apoc.cypher.runSchemaFile(file or url,[{statistics:true,timeout:10}]) - allows only schema operations, runs each schema statement in the file, all semicolon separated")
    public Stream<RowResult> runSchemaFile(@Name("file") String fileName, @Name(value = "config",defaultValue = "{}") Map<String,Object> config) {
        return runSchemaFiles(singletonList(fileName),config);
    }

    @Procedure(mode=Mode.SCHEMA)
    @Description("apoc.cypher.runSchemaFiles([files or urls],{statistics:true,timeout:10}) - allows only schema operations, runs each schema statement in the files, all semicolon separated")
    public Stream<RowResult> runSchemaFiles(@Name("file") List<String> fileNames, @Name(value = "config",defaultValue = "{}") Map<String,Object> config) {
        boolean addStatistics = Util.toBoolean(config.getOrDefault("statistics",true));
        int timeout = Util.toInteger(config.getOrDefault("timeout",10));
        List<RowResult> result = new ArrayList<>();
        for (String f : fileNames) {
            List<RowResult> rowResults = runManyStatements(readerForFile(f), Collections.emptyMap(), true, addStatistics, timeout).collect(Collectors.toList());
            result.addAll(rowResults);
        }
        return result.stream();
    }

    private Stream<RowResult> runManyStatements(Reader reader, Map<String, Object> params, boolean schemaOperation, boolean addStatistics, int timeout) {
        BlockingQueue<RowResult> queue = new ArrayBlockingQueue<>(100);
        Util.inThread(() -> {
            if (schemaOperation) {
                runSchemaStatementsInTx(reader, queue, params, addStatistics,timeout);
            } else {
                runDataStatementsInTx(reader, queue, params, addStatistics,timeout);
            }
            queue.put(RowResult.TOMBSTONE);
            return null;
        });
        return StreamSupport.stream(new QueueBasedSpliterator<>(queue, RowResult.TOMBSTONE, terminationGuard, timeout), false);
    }

    private void runDataStatementsInTx(Reader reader, BlockingQueue<RowResult> queue, Map<String, Object> params, boolean addStatistics, long timeout) {
        Scanner scanner = new Scanner(reader);
        scanner.useDelimiter(";\r?\n");
        while (scanner.hasNext()) {
            String stmt = removeShellControlCommands(scanner.next());
            if (stmt.trim().isEmpty()) continue;
            if (!isSchemaOperation(stmt)) {
                if (isPeriodicOperation(stmt))
                    Util.inThread(() -> executeStatement(queue, stmt, params, addStatistics,timeout));
                else Util.inTx(db, () -> executeStatement(queue, stmt, params, addStatistics,timeout));
            }
        }
    }

    private void runSchemaStatementsInTx(Reader reader, BlockingQueue<RowResult> queue, Map<String, Object> params, boolean addStatistics, long timeout) {
        Scanner scanner = new Scanner(reader);
        scanner.useDelimiter(";\r?\n");
        while (scanner.hasNext()) {
            String stmt = removeShellControlCommands(scanner.next());
            if (stmt.trim().isEmpty()) continue;
            if (isSchemaOperation(stmt)) {
                Util.inTx(db, () -> executeStatement(queue, stmt, params, addStatistics, timeout));
            }
        }
    }

    @Procedure(mode = WRITE)
    @Description("apoc.cypher.runMany('cypher;\\nstatements;',{params},[{statistics:true,timeout:10}]) - runs each semicolon separated statement and returns summary - currently no schema operations")
    public Stream<RowResult> runMany(@Name("cypher") String cypher, @Name("params") Map<String,Object> params, @Name(value = "config",defaultValue = "{}") Map<String,Object> config) {
        boolean addStatistics = Util.toBoolean(config.getOrDefault("statistics",true));
        int timeout = Util.toInteger(config.getOrDefault("timeout",1));
        StringReader stringReader = new StringReader(cypher);
        return runManyStatements(stringReader ,params, false, addStatistics, timeout);
    }

    private final static Pattern shellControl = Pattern.compile("^:?\\b(begin|commit|rollback)\\b", Pattern.CASE_INSENSITIVE);

    private Object executeStatement(BlockingQueue<RowResult> queue, String stmt, Map<String, Object> params, boolean addStatistics, long timeout) throws InterruptedException {
        try (Result result = db.execute(stmt,params)) {
            long time = System.currentTimeMillis();
            int row = 0;
            while (result.hasNext()) {
                terminationGuard.check();
                queue.put(new RowResult(row++, result.next()));
            }
            if (addStatistics) {
                queue.offer(new RowResult(-1, toMap(result.getQueryStatistics(), System.currentTimeMillis() - time, row)), timeout,TimeUnit.SECONDS);
            }
            return row;
        }
    }

    private String removeShellControlCommands(String stmt) {
        Matcher matcher = shellControl.matcher(stmt.trim());
        if (matcher.find()) {
            // an empty file get transformed into ":begin\n:commit" and that statement is not matched by the pattern
            // because ":begin\n:commit".replaceAll("") => "\n:commit" with the recursion we avoid the problem
            return removeShellControlCommands(matcher.replaceAll(""));
        }
        return stmt;
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
            return FileUtils.readerFor(fileName);
        } catch (IOException ioe) {
            throw new RuntimeException("Error accessing file "+fileName,ioe);
        }
    }

    public static String withParamMapping(String fragment, Collection<String> keys) {
        if (keys.isEmpty()) return fragment;
        String declaration = " WITH " + join(", ", keys.stream().map(s -> format(" {`%s`} as `%s` ", s, s)).collect(toList()));
        return declaration + fragment;
    }

    public static String compiled(String fragment) {
        return fragment.substring(0,6).equalsIgnoreCase("cypher") ? fragment : COMPILED_PREFIX + fragment;
    }

    @Procedure
    public Stream<MapResult> parallel(@Name("fragment") String fragment, @Name("params") Map<String, Object> params, @Name("parallelizeOn") String key) {
        if (params == null) return run(fragment, params);
        if (key == null || !params.containsKey(key))
            throw new RuntimeException("Can't parallelize on key " + key + " available keys " + params.keySet());
        Object value = params.get(key);
        if (!(value instanceof Collection))
            throw new RuntimeException("Can't parallelize a non collection " + key + " : " + value);

        final String statement = withParamMapping(fragment, params.keySet());
        Collection<Object> coll = (Collection<Object>) value;
        return coll.parallelStream().flatMap((v) -> {
            terminationGuard.check();
            Map<String, Object> parallelParams = new HashMap<>(params);
            parallelParams.replace(key, v);
            return db.execute(statement, parallelParams).stream().map(MapResult::new);
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
        final String statement = withParamsAndIterator(fragment, params.keySet(), "_");
        db.execute("EXPLAIN " + statement).close();
        return Util.partitionSubList(data, PARTITIONS,null)
                .flatMap((partition) -> Iterators.addToCollection(db.execute(statement, parallelParams(params, "_", partition)),
                        new ArrayList<>(partition.size())).stream())
                .map(MapResult::new);
    }
    @Procedure
    @Description("apoc.cypher.mapParallel2(fragment, params, list-to-parallelize) yield value - executes fragment in parallel batches with the list segments being assigned to _")
    public Stream<MapResult> mapParallel2(@Name("fragment") String fragment, @Name("params") Map<String, Object> params, @Name("list") List<Object> data, @Name("partitions") long partitions,@Name(value = "timeout",defaultValue = "10") long timeout) {
        final String statement = withParamsAndIterator(fragment, params.keySet(), "_");
        db.execute("EXPLAIN " + statement).close();
        BlockingQueue<RowResult> queue = new ArrayBlockingQueue<>(100000);
        Stream<List<Object>> parallelPartitions = Util.partitionSubList(data, (int)(partitions <= 0 ? PARTITIONS : partitions), null);
        Util.inFuture(() -> {
            long total = parallelPartitions
                .map((List<Object> partition) -> {
                    try {
                        return executeStatement(queue, statement, parallelParams(params, "_", partition),false,timeout);
                    } catch (Exception e) {throw new RuntimeException(e);}}
                ).count();
            queue.put(RowResult.TOMBSTONE);
            return total;
        });
        return StreamSupport.stream(new QueueBasedSpliterator<>(queue, RowResult.TOMBSTONE, terminationGuard, timeout),true).map((rowResult) -> new MapResult(rowResult.result));
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

        final String statement = withParamsAndIterator(fragment, params.keySet(), key);
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
                terminationGuard.check();
                futures.add(submit(db, statement, params, key, partition));
                partition = new ArrayList<>(batchSize);
            }
        }
        if (!partition.isEmpty()) {
            futures.add(submit(db, statement, params, key, partition));
        }
        return futures.stream().flatMap(f -> {
            try {
                return f.get().stream().map(MapResult::new);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Error executing in parallel " + statement, e);
            }
        });
    }

    public static String withParamsAndIterator(String fragment, Collection<String> params, String iterator) {
        boolean noIterator = iterator == null || iterator.isEmpty();
        if (params.isEmpty() && noIterator) return fragment;
        String with = Util.withMapping(params.stream().filter((c) -> noIterator || !c.equals(iterator)), (c) -> param(c) + " AS " + quote(c));
        if (noIterator) return with + fragment;
        return with + " UNWIND " + param(iterator) + " AS " + quote(iterator) + ' ' + fragment;
    }

    private Future<List<Map<String, Object>>> submit(GraphDatabaseService db, String statement, Map<String, Object> params, String key, List<Object> partition) {
        return POOL.submit(() -> Iterators.addToCollection(db.execute(statement, parallelParams(params, key, partition)), new ArrayList<>(partition.size())));
    }

    private static Collection asCollection(Object value) {
        if (value instanceof Collection) return (Collection) value;
        if (value instanceof Iterable) return Iterables.asCollection((Iterable) value);
        if (value instanceof Iterator) return Iterators.asCollection((Iterator) value);
        return Collections.singleton(value);
    }

    @Procedure(mode = WRITE)
    @Description("apoc.cypher.doIt(fragment, params) yield value - executes writing fragment with the given parameters")
    public Stream<MapResult> doIt(@Name("cypher") String statement, @Name("params") Map<String, Object> params) {
        if (params == null) params = Collections.emptyMap();
        return db.execute(withParamMapping(statement, params.keySet()), params).stream().map(MapResult::new);
    }

    @Procedure("apoc.when")
    @Description("apoc.when(condition, ifQuery, elseQuery:'', params:{}) yield value - based on the conditional, executes read-only ifQuery or elseQuery with the given parameters")
    public Stream<MapResult> when(@Name("condition") boolean condition, @Name("ifQuery") String ifQuery, @Name(value="elseQuery", defaultValue = "") String elseQuery, @Name(value="params", defaultValue = "") Map<String, Object> params) {
        if (params == null) params = Collections.emptyMap();
        String targetQuery = condition ? ifQuery : elseQuery;

        if (targetQuery.isEmpty()) {
            return Stream.of(new MapResult(Collections.emptyMap()));
        } else {
            return db.execute(withParamMapping(targetQuery, params.keySet()), params).stream().map(MapResult::new);
        }
    }

    @Procedure(value="apoc.do.when", mode = Mode.WRITE)
    @Description("apoc.do.when(condition, ifQuery, elseQuery:'', params:{}) yield value - based on the conditional, executes writing ifQuery or elseQuery with the given parameters")
    public Stream<MapResult> doWhen(@Name("condition") boolean condition, @Name("ifQuery") String ifQuery, @Name(value="elseQuery", defaultValue = "") String elseQuery, @Name(value="params", defaultValue = "") Map<String, Object> params) {
        return when(condition, ifQuery, elseQuery, params);
    }

    @Procedure("apoc.case")
    @Description("apoc.case([condition, query, condition, query, ...], elseQuery:'', params:{}) yield value - given a list of conditional / read-only query pairs, executes the query associated with the first conditional evaluating to true (or the else query if none are true) with the given parameters")
    public Stream<MapResult> whenCase(@Name("conditionals") List<Object> conditionals, @Name(value="elseQuery", defaultValue = "") String elseQuery, @Name(value="params", defaultValue = "") Map<String, Object> params) {
        if (params == null) params = Collections.emptyMap();

        if (conditionals.size() % 2 != 0) {
            throw new IllegalArgumentException("Conditionals must be an even-sized collection of boolean, query entries");
        }

        Iterator caseItr = conditionals.iterator();

        while (caseItr.hasNext()) {
            boolean condition = (Boolean) caseItr.next();
            String ifQuery = (String) caseItr.next();

            if (condition) {
                return db.execute(withParamMapping(ifQuery, params.keySet()), params).stream().map(MapResult::new);
            }
        }

        if (elseQuery.isEmpty()) {
            return Stream.of(new MapResult(Collections.emptyMap()));
        } else {
            return db.execute(withParamMapping(elseQuery, params.keySet()), params).stream().map(MapResult::new);
        }
    }

    @Procedure(value="apoc.do.case", mode = Mode.WRITE)
    @Description("apoc.do.case([condition, query, condition, query, ...], elseQuery:'', params:{}) yield value - given a list of conditional / writing query pairs, executes the query associated with the first conditional evaluating to true (or the else query if none are true) with the given parameters")
    public Stream<MapResult> doWhenCase(@Name("conditionals") List<Object> conditionals, @Name(value="elseQuery", defaultValue = "") String elseQuery, @Name(value="params", defaultValue = "") Map<String, Object> params) {
        return whenCase(conditionals, elseQuery, params);
    }
}
