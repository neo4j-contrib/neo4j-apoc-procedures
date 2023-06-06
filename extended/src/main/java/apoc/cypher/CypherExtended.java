package apoc.cypher;

import apoc.Extended;
import apoc.Pools;
import apoc.result.MapResult;
import apoc.util.CompressionAlgo;
import apoc.util.EntityUtil;
import apoc.util.FileUtils;
import apoc.util.QueueBasedSpliterator;
import apoc.util.QueueUtil;
import apoc.util.Util;
import apoc.util.collection.Iterators;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.cypher.CypherUtils.runCypherQuery;
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
@Extended
public class CypherExtended {
    public static final String COMPILED_PREFIX = "CYPHER runtime=interpreted"; // todo handle enterprise properly
    public static final int PARTITIONS = 100 * Runtime.getRuntime().availableProcessors();
    public static final int MAX_BATCH = 10000;

    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Context
    public TerminationGuard terminationGuard;

    @Context
    public Pools pools;

    @Procedure(mode = WRITE)
    @Description("apoc.cypher.runFile(file or url,[{statistics:true,timeout:10,parameters:{}}]) - runs each statement in the file, all semicolon separated - currently no schema operations")
    public Stream<RowResult> runFile(@Name("file") String fileName, @Name(value = "config",defaultValue = "{}") Map<String,Object> config) {
        return runFiles(singletonList(fileName),config);
    }

    @Procedure(mode = WRITE)
    @Description("apoc.cypher.runFiles([files or urls],[{statistics:true,timeout:10,parameters:{}}])) - runs each statement in the files, all semicolon separated")
    public Stream<RowResult> runFiles(@Name("file") List<String> fileNames, @Name(value = "config",defaultValue = "{}") Map<String,Object> config) {
        @SuppressWarnings( "unchecked" )
        final Map<String,Object> parameters = (Map<String,Object>) config.getOrDefault("parameters",Collections.emptyMap());
        final boolean schemaOperation = false;
        return runFiles(fileNames, config, parameters, schemaOperation);
    }

    // This runs the files sequentially
    private Stream<RowResult> runFiles(List<String> fileNames, Map<String, Object> config, Map<String, Object> parameters, boolean schemaOperation) {
        boolean reportError = Util.toBoolean(config.get("reportError"));
        boolean addStatistics = Util.toBoolean(config.getOrDefault("statistics",true));
        int timeout = Util.toInteger(config.getOrDefault("timeout",10));
        int queueCapacity = Util.toInteger(config.getOrDefault("queueCapacity",100));
        var result = fileNames.stream().flatMap(fileName -> {
            final Reader reader = readerForFile(fileName);
            final Scanner scanner = createScannerFor(reader);
            return runManyStatements(scanner, parameters, schemaOperation, addStatistics, timeout, queueCapacity, reportError, fileName)
                    .onClose(() -> Util.close(scanner, (e) -> log.info("Cannot close the scanner for file " + fileName + " because the following exception", e)));
        });

        return result;
    }

    @Procedure(mode=Mode.SCHEMA)
    @Description("apoc.cypher.runSchemaFile(file or url,[{statistics:true,timeout:10}]) - allows only schema operations, runs each schema statement in the file, all semicolon separated")
    public Stream<RowResult> runSchemaFile(@Name("file") String fileName, @Name(value = "config",defaultValue = "{}") Map<String,Object> config) {
        return runSchemaFiles(singletonList(fileName),config);
    }

    @Procedure(mode=Mode.SCHEMA)
    @Description("apoc.cypher.runSchemaFiles([files or urls],{statistics:true,timeout:10}) - allows only schema operations, runs each schema statement in the files, all semicolon separated")
    public Stream<RowResult> runSchemaFiles(@Name("file") List<String> fileNames, @Name(value = "config",defaultValue = "{}") Map<String,Object> config) {
        final boolean schemaOperation = true;
        final Map<String, Object> parameters = Collections.emptyMap();
        return runFiles(fileNames, config, parameters, schemaOperation);
    }

    private Stream<RowResult> runManyStatements(Scanner scanner, Map<String, Object> params, boolean schemaOperation, boolean addStatistics, int timeout, int queueCapacity, boolean reportError, String fileName) {
        BlockingQueue<RowResult> queue = runInSeparateThreadAndSendTombstone(queueCapacity, internalQueue -> {
            if (schemaOperation) {
                runSchemaStatementsInTx(scanner, internalQueue, params, addStatistics, timeout, reportError, fileName);
            } else {
                runDataStatementsInTx(scanner, internalQueue, params, addStatistics, timeout, reportError, fileName);
            }
        }, RowResult.TOMBSTONE);
        return StreamSupport.stream(new QueueBasedSpliterator<>(queue, RowResult.TOMBSTONE, terminationGuard, Integer.MAX_VALUE), false);
    }


    private <T> BlockingQueue<T> runInSeparateThreadAndSendTombstone(int queueCapacity, Consumer<BlockingQueue<T>> action, T tombstone) {
        /* NB: this must not be called via an existing thread pool - otherwise we could run into a deadlock
           other jobs using the same pool might completely exhaust at and the thread sending TOMBSTONE will
           wait in the pool's job queue.
         */
        BlockingQueue<T> queue = new ArrayBlockingQueue<>(queueCapacity);
        Util.newDaemonThread(() -> {
            try {
                action.accept(queue);
            } finally {
                while (true) {  // ensure we send TOMBSTONE even if there's an InterruptedException
                    try {
                        queue.put(tombstone);
                        return;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }).start();
        return queue;
    }

    private void runDataStatementsInTx(Scanner scanner, BlockingQueue<RowResult> queue, Map<String, Object> params, boolean addStatistics, long timeout, boolean reportError, String fileName) {
        while (scanner.hasNext()) {
            String stmt = removeShellControlCommands(scanner.next());
            if (stmt.trim().isEmpty()) continue;
            boolean schemaOperation;
            try {
                schemaOperation = isSchemaOperation(stmt);
            } catch (Exception e) {
                collectError(queue, reportError, e, fileName);
                return;
            }

            if (!schemaOperation) {
                if (isPeriodicOperation(stmt)) {
                    Util.inThread(pools , () -> {
                        try {
                            return db.executeTransactionally(stmt, params, result -> consumeResult(result, queue, addStatistics, timeout));
                        } catch (Exception e) {
                            collectError(queue, reportError, e, fileName);
                            return null;
                        }
                    });
                }
                else {
                    Util.inTx(db, pools, threadTx -> {
                        try (Result result = threadTx.execute(stmt, params)) {
                            return consumeResult(result, queue, addStatistics, timeout);
                        } catch (Exception e) {
                            collectError(queue, reportError, e, fileName);
                            return null;
                        }
                    });
                }
            }
        }
    }

    private void collectError(BlockingQueue<RowResult> queue, boolean reportError, Exception e, String fileName) {
        if (!reportError) {
            throw new RuntimeException(e);
        }
        String error = String.format("Error in `%s`:\n%s ",
                fileName, e.getMessage()
        );

        RowResult result = new RowResult(-1, Map.of("error", error));
        QueueUtil.put(queue, result, 10);
    }

    private Scanner createScannerFor(Reader reader) {
        Scanner scanner = new Scanner(reader);
        scanner.useDelimiter(";\r?\n");
        return scanner;
    }

    private void runSchemaStatementsInTx(Scanner scanner, BlockingQueue<RowResult> queue, Map<String, Object> params, boolean addStatistics, long timeout, boolean reportError, String fileName) {
        while (scanner.hasNext()) {
            String stmt = removeShellControlCommands(scanner.next());
            if (stmt.trim().isEmpty()) continue;
            boolean schemaOperation;
            try {
                schemaOperation = isSchemaOperation(stmt);
            } catch (Exception e) {
                collectError(queue, reportError, e, fileName);
                return;
            }
            if (schemaOperation) {
                Util.inTx(db, pools, txInThread -> {
                    try (Result result = txInThread.execute(stmt, params)) {
                        return consumeResult(result, queue, addStatistics, timeout);
                    } catch (Exception e) {
                        collectError(queue, reportError, e, fileName);
                        return null;
                    }
                });
            }
        }
    }

    private final static Pattern shellControl = Pattern.compile("^:?\\b(begin|commit|rollback)\\b", Pattern.CASE_INSENSITIVE);

    private Object consumeResult(Result result, BlockingQueue<RowResult> queue, boolean addStatistics, long timeout) {
        try {
            long time = System.currentTimeMillis();
            int row = 0;
            while (result.hasNext()) {
                terminationGuard.check();
                Map<String, Object> res = EntityUtil.anyRebind(tx, result.next());
                queue.put(new RowResult(row++, res));
            }
            if (addStatistics) {
                queue.put(new RowResult(-1, toMap(result.getQueryStatistics(), System.currentTimeMillis() - time, row)));
            }
            return row;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
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

    private boolean isSchemaOperation(String statement) {
        return db.executeTransactionally("EXPLAIN " + statement, Collections.emptyMap(),
                res -> QueryExecutionType.QueryType.SCHEMA_WRITE.equals(res.getQueryExecutionType().queryType())
        );
    }
    private boolean isPeriodicOperation(String stmt) {
        return stmt.matches("(?is).*using\\s+periodic.*") || stmt.matches("(?is).*in\\s+transactions.*");
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
            return FileUtils.readerFor(fileName, CompressionAlgo.NONE.name());
        } catch (IOException ioe) {
            throw new RuntimeException("Error accessing file "+fileName,ioe);
        }
    }

    public static String withParamMapping(String fragment, Collection<String> keys) {
        if (keys.isEmpty()) return fragment;
        String declaration = " WITH " + join(", ", keys.stream().map(s -> format(" $`%s` as `%s` ", s, s)).collect(toList()));
        return declaration + fragment;
    }

    public static String compiled(String fragment) {
        return fragment.substring(0,6).equalsIgnoreCase("cypher") ? fragment : COMPILED_PREFIX + fragment;
    }

    @Procedure
    @Description("apoc.cypher.parallel(fragment, `paramMap`, `keyList`) yield value - executes fragments in parallel through a list defined in `paramMap` with a key `keyList`")
    public Stream<MapResult> parallel(@Name("fragment") String fragment, @Name("params") Map<String, Object> params, @Name("parallelizeOn") String key) {
        if (params == null) return runCypherQuery(tx, fragment, params);
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
            return tx.execute(statement, parallelParams).stream().map(MapResult::new);
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
        tx.execute("EXPLAIN " + statement).close();
        return Util.partitionSubList(data, PARTITIONS,null)
                .flatMap((partition) -> Iterators.asList(tx.execute(statement, parallelParams(params, "_", partition))).stream())
                .map(MapResult::new);
    }
    @Procedure
    @Description("apoc.cypher.mapParallel2(fragment, params, list-to-parallelize) yield value - executes fragment in parallel batches with the list segments being assigned to _")
    public Stream<MapResult> mapParallel2(@Name("fragment") String fragment, @Name("params") Map<String, Object> params, @Name("list") List<Object> data, @Name("partitions") long partitions,@Name(value = "timeout",defaultValue = "10") long timeout) {
        final String statement = withParamsAndIterator(fragment, params.keySet(), "_");
        tx.execute("EXPLAIN " + statement).close();
        BlockingQueue<RowResult> queue = new ArrayBlockingQueue<>(100000);
        Stream<List<Object>> parallelPartitions = Util.partitionSubList(data, (int)(partitions <= 0 ? PARTITIONS : partitions), null);
        Util.inFuture(pools, () -> {
            long total = parallelPartitions
                .map((List<Object> partition) -> {
                    try (Transaction transaction = db.beginTx();
                         Result result = transaction.execute(statement, parallelParams(params, "_", partition))) {
                        return consumeResult(result, queue, false, timeout);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }}
                ).count();
            queue.put(RowResult.TOMBSTONE);
            return total;
        });
        return StreamSupport.stream(new QueueBasedSpliterator<>(queue, RowResult.TOMBSTONE, terminationGuard, (int)timeout),true).map((rowResult) -> new MapResult(rowResult.result));
    }

    public Map<String, Object> parallelParams(@Name("params") Map<String, Object> params, String key, List<Object> partition) {
        if (params.isEmpty()) return Collections.singletonMap(key, partition);

        Map<String, Object> parallelParams = new HashMap<>(params);
        parallelParams.put(key, partition);
        return parallelParams;
    }

    @Procedure
    @Description("apoc.cypher.parallel2(fragment, `paramMap`, `keyList`) yield value - executes fragments in parallel batches through a list defined in `paramMap` with a key `keyList`")
    public Stream<MapResult> parallel2(@Name("fragment") String fragment, @Name("params") Map<String, Object> params, @Name("parallelizeOn") String key) {
        if (params == null) return runCypherQuery(tx, fragment, params);
        if (StringUtils.isEmpty(key) || !params.containsKey(key))
            throw new RuntimeException("Can't parallelize on key " + key + " available keys " + params.keySet() + ". Note that parallelizeOn parameter must be not empty");
        Object value = params.get(key);
        if (!(value instanceof Collection))
            throw new RuntimeException("Can't parallelize a non collection " + key + " : " + value);

        final String statement = withParamsAndIterator(fragment, params.keySet(), key);
        tx.execute("EXPLAIN " + statement).close();
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
                futures.add(submit(db, statement, params, key, partition, terminationGuard));
                partition = new ArrayList<>(batchSize);
            }
        }
        if (!partition.isEmpty()) {
            futures.add(submit(db, statement, params, key, partition, terminationGuard));
        }
        return futures.stream().flatMap(f -> {
            try {
                return EntityUtil.anyRebind(tx, f.get()).stream().map(MapResult::new);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Error executing in parallel " + statement, e);
            }
        });
    }

    public static String withParamsAndIterator(String fragment, Collection<String> params, String iterator) {
        String with = Util.withMapping(params.stream().filter((c) -> !c.equals(iterator)), (c) -> param(c) + " AS " + quote(c));
        return with + " UNWIND " + param(iterator) + " AS " + quote(iterator) + ' ' + fragment;
    }

    private Future<List<Map<String, Object>>> submit(GraphDatabaseService db, String statement, Map<String, Object> params, String key, List<Object> partition, TerminationGuard terminationGuard) {
        return pools.getDefaultExecutorService().submit(
                () -> {
                    terminationGuard.check();
                    return db.executeTransactionally(statement, parallelParams(params, key, partition), result -> Iterators.asList(result));
                }
        );
    }
}
