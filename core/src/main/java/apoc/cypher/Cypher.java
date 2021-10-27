package apoc.cypher;

import apoc.Pools;
import apoc.result.MapResult;
import apoc.util.QueueBasedSpliterator;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
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

import java.io.Reader;
import java.io.StringReader;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.util.MapUtil.map;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.stream.Collectors.toList;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.SCHEMA;
import static org.neo4j.procedure.Mode.WRITE;

/**
 * @author mh
 * @since 08.05.16
 */
public class Cypher {

    public static final String COMPILED_PREFIX = "CYPHER runtime="+ Util.COMPILED;
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

    @Procedure
    @Description("apoc.cypher.run(fragment, params) yield value - executes reading fragment with the given parameters - currently no schema operations")
    public Stream<MapResult> run(@Name("cypher") String statement, @Name("params") Map<String, Object> params) {
        return runCypherQuery(tx, statement, params);
    }

    public static Stream<MapResult> runCypherQuery(Transaction tx, @Name("cypher") String statement, @Name("params") Map<String, Object> params) {
        if (params == null) params = Collections.emptyMap();
        return tx.execute(withParamMapping(statement, params.keySet()), params).stream().map(MapResult::new);
    }


    private Stream<RowResult> runManyStatements(Reader reader, Map<String, Object> params, boolean schemaOperation, boolean addStatistics, int timeout, int queueCapacity) {
        BlockingQueue<RowResult> queue = runInSeparateThreadAndSendTombstone(queueCapacity, internalQueue -> {
            if (schemaOperation) {
                runSchemaStatementsInTx(reader, internalQueue, params, addStatistics, timeout);
            } else {
                runDataStatementsInTx(reader, internalQueue, params, addStatistics, timeout);
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

    private void runDataStatementsInTx(Reader reader, BlockingQueue<RowResult> queue, Map<String, Object> params, boolean addStatistics, long timeout) {
        Scanner scanner = new Scanner(reader);
        scanner.useDelimiter(";\r?\n");
        while (scanner.hasNext()) {
            String stmt = removeShellControlCommands(scanner.next());
            if (stmt.trim().isEmpty()) continue;
            if (!isSchemaOperation(stmt)) {
                if (isPeriodicOperation(stmt)) {
                    Util.inThread(pools , () -> db.executeTransactionally(stmt, params, result -> consumeResult(result, queue, addStatistics, timeout)));
                }
                else {
                    Util.inTx(db, pools, threadTx -> {
                        try (Result result = threadTx.execute(stmt, params)) {
                            return consumeResult(result, queue, addStatistics, timeout);
                        }
                    });
                }
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
                Util.inTx(db, pools, txInThread -> {
                    try (Result result = txInThread.execute(stmt, params)) {
                        return consumeResult(result, queue, addStatistics, timeout);
                    }
                });
            }
        }
    }

    @Procedure(mode = WRITE)
    @Description("apoc.cypher.runMany('cypher;\\nstatements;', $params, [{statistics:true,timeout:10}]) - runs each semicolon separated statement and returns summary - currently no schema operations")
    public Stream<RowResult> runMany(@Name("cypher") String cypher, @Name("params") Map<String,Object> params, @Name(value = "config",defaultValue = "{}") Map<String,Object> config) {
        boolean addStatistics = Util.toBoolean(config.getOrDefault("statistics",true));
        int timeout = Util.toInteger(config.getOrDefault("timeout",1));
        int queueCapacity = Util.toInteger(config.getOrDefault("queueCapacity",100));

        StringReader stringReader = new StringReader(cypher);
        return runManyStatements(stringReader ,params, false, addStatistics, timeout, queueCapacity);
    }

    @Procedure(mode = READ)
    @Description("apoc.cypher.runManyReadOnly('cypher;\\nstatements;', $params, [{statistics:true,timeout:10}]) - runs each semicolon separated, read-only statement and returns summary - currently no schema operations")
    public Stream<RowResult> runManyReadOnly(@Name("cypher") String cypher, @Name("params") Map<String,Object> params, @Name(value = "config",defaultValue = "{}") Map<String,Object> config) {
        return runMany(cypher, params, config);
    }

    private final static Pattern shellControl = Pattern.compile("^:?\\b(begin|commit|rollback)\\b", Pattern.CASE_INSENSITIVE);

    private Object consumeResult(Result result, BlockingQueue<RowResult> queue, boolean addStatistics, long timeout) {
        try {
            long time = System.currentTimeMillis();
            int row = 0;
            while (result.hasNext()) {
                terminationGuard.check();
                queue.put(new RowResult(row++, result.next()));
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

    public static String withParamMapping(String fragment, Collection<String> keys) {
        if (keys.isEmpty()) return fragment;
        String declaration = " WITH " + join(", ", keys.stream().map(s -> format(" $`%s` as `%s` ", s, s)).collect(toList()));
        return declaration + fragment;
    }

    @Procedure(mode = WRITE)
    @Description("apoc.cypher.doIt(fragment, params) yield value - executes writing fragment with the given parameters")
    public Stream<MapResult> doIt(@Name("cypher") String statement, @Name("params") Map<String, Object> params) {
        return runCypherQuery(tx, statement, params);
    }

    @Procedure(mode = WRITE)
    @Description("apoc.cypher.runWrite(statement, params) yield value - alias for apoc.cypher.doIt")
    public Stream<MapResult> runWrite(@Name("cypher") String statement, @Name("params") Map<String, Object> params) {
        return doIt(statement, params);
    }

    @Procedure(mode = SCHEMA)
    @Description("apoc.cypher.runSchema(statement, params) yield value - executes query schema statement with the given parameters")
    public Stream<MapResult> runSchema(@Name("cypher") String statement, @Name("params") Map<String, Object> params) {
        return runCypherQuery(tx, statement, params);
    }

    @Procedure("apoc.when")
    @Description("apoc.when(condition, ifQuery, elseQuery:'', params:{}) yield value - based on the conditional, executes read-only ifQuery or elseQuery with the given parameters")
    public Stream<MapResult> when(@Name("condition") boolean condition, @Name("ifQuery") String ifQuery, @Name(value="elseQuery", defaultValue = "") String elseQuery, @Name(value="params", defaultValue = "{}") Map<String, Object> params) {
        if (params == null) params = Collections.emptyMap();
        String targetQuery = condition ? ifQuery : elseQuery;

        if (targetQuery.isEmpty()) {
            return Stream.of(new MapResult(Collections.emptyMap()));
        } else {
            return tx.execute(withParamMapping(targetQuery, params.keySet()), params).stream().map(MapResult::new);
        }
    }

    @Procedure(value="apoc.do.when", mode = Mode.WRITE)
    @Description("apoc.do.when(condition, ifQuery, elseQuery:'', params:{}) yield value - based on the conditional, executes writing ifQuery or elseQuery with the given parameters")
    public Stream<MapResult> doWhen(@Name("condition") boolean condition, @Name("ifQuery") String ifQuery, @Name(value="elseQuery", defaultValue = "") String elseQuery, @Name(value="params", defaultValue = "{}") Map<String, Object> params) {
        return when(condition, ifQuery, elseQuery, params);
    }

    @Procedure("apoc.case")
    @Description("apoc.case([condition, query, condition, query, ...], elseQuery:'', params:{}) yield value - given a list of conditional / read-only query pairs, executes the query associated with the first conditional evaluating to true (or the else query if none are true) with the given parameters")
    public Stream<MapResult> whenCase(@Name("conditionals") List<Object> conditionals, @Name(value="elseQuery", defaultValue = "") String elseQuery, @Name(value="params", defaultValue = "{}") Map<String, Object> params) {
        if (params == null) params = Collections.emptyMap();

        if (conditionals.size() % 2 != 0) {
            throw new IllegalArgumentException("Conditionals must be an even-sized collection of boolean, query entries");
        }

        Iterator caseItr = conditionals.iterator();

        while (caseItr.hasNext()) {
            boolean condition = (Boolean) caseItr.next();
            String ifQuery = (String) caseItr.next();

            if (condition) {
                return tx.execute(withParamMapping(ifQuery, params.keySet()), params).stream().map(MapResult::new);
            }
        }

        if (elseQuery.isEmpty()) {
            return Stream.of(new MapResult(Collections.emptyMap()));
        } else {
            return tx.execute(withParamMapping(elseQuery, params.keySet()), params).stream().map(MapResult::new);
        }
    }

    @Procedure(value="apoc.do.case", mode = Mode.WRITE)
    @Description("apoc.do.case([condition, query, condition, query, ...], elseQuery:'', params:{}) yield value - given a list of conditional / writing query pairs, executes the query associated with the first conditional evaluating to true (or the else query if none are true) with the given parameters")
    public Stream<MapResult> doWhenCase(@Name("conditionals") List<Object> conditionals, @Name(value="elseQuery", defaultValue = "") String elseQuery, @Name(value="params", defaultValue = "{}") Map<String, Object> params) {
        return whenCase(conditionals, elseQuery, params);
    }
}
