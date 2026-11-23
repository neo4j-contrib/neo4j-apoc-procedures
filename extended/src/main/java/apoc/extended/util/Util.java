/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.extended.util;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.DateFormatUtil.getOrCreate;
import static org.eclipse.jetty.util.URIUtil.encodePath;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

import apoc.Pools;
import apoc.convert.ConvertUtils;
import apoc.export.util.ExportConfig;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.util.collection.Iterators;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.lang.model.SourceVersion;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.TerminationGuard;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;
import apoc.util.CompressionAlgo;
import apoc.util.FileUtils;
import apoc.util.JsonUtil;
import apoc.util.MapUtil;
import apoc.util.StreamConnection;

/**
 * @author mh
 * @since 24.04.16
 */
public class Util {

    private static final boolean PRINT_CLASS_CHECK_STACK_TRACES = Boolean.getBoolean("printClassCheckStackTracers");
    public static final String NODE_COUNT = "MATCH (n) RETURN count(*) as result";
    public static final String REL_COUNT = "MATCH ()-->() RETURN count(*) as result";

    public static String INVALID_QUERY_MODE_ERROR = "This procedure allows for READ-only, non-schema based queries. "
            + "It is therefore not possible to perform writes or query the database with commands such as SHOW CONSTRAINTS/INDEXES.";

    public static String labelString(Node n) {
        return joinLabels(n.getLabels(), ":");
    }

    public static String joinLabels(Iterable<Label> labels, String s) {
        return StreamSupport.stream(labels.spliterator(), false)
                .map(Label::name)
                .collect(Collectors.joining(s));
    }

    public static List<String> labelStrings(Node n) {
        return StreamSupport.stream(n.getLabels().spliterator(), false)
                .map(Label::name)
                .sorted()
                .collect(Collectors.toList());
    }

    public static Stream<Object> stream(Object values) {
        return ConvertUtils.convertToList(values).stream();
    }

    public static Stream<Node> nodeStream(InternalTransaction tx, Object ids) {
        return stream(ids).map(id -> node(tx, id));
    }

    public static Node node(InternalTransaction tx, Object id) {
        if (id instanceof Node node) return rebind(tx, node);
        if (id instanceof Number nodeId)
            return tx.getNodeByElementId(tx.elementIdMapper().nodeElementId(nodeId.longValue()));
        if (id instanceof String elementId) return tx.getNodeByElementId(elementId);
        throw new RuntimeException("Can't convert " + id.getClass() + " to a Node");
    }

    public static Stream<Relationship> relsStream(InternalTransaction tx, Object ids) {
        return stream(ids).map(id -> relationship(tx, id));
    }

    public static Relationship relationship(InternalTransaction tx, Object id) {
        if (id instanceof Relationship rel) return rebind(tx, rel);
        if (id instanceof Number relId)
            return tx.getRelationshipByElementId(tx.elementIdMapper().relationshipElementId(relId.longValue()));
        if (id instanceof String elementId) return tx.getRelationshipByElementId(elementId);
        throw new RuntimeException("Can't convert " + id.getClass() + " to a Relationship");
    }

    public static <T> T retryInTx(
            Log log,
            GraphDatabaseService db,
            Function<Transaction, T> function,
            long retry,
            long maxRetries,
            Consumer<Long> callbackForRetry) {
        try (Transaction tx = db.beginTx()) {
            T result = function.apply(tx);
            tx.commit();
            return result;
        } catch (Exception e) {
            if (retry >= maxRetries) throw e;
            if (log != null) {
                log.warn("Retrying operation %d of %d", retry, maxRetries);
            }
            callbackForRetry.accept(retry);
            Util.sleep(100);
            return retryInTx(log, db, function, retry + 1, maxRetries, callbackForRetry);
        }
    }

    public static <T> Future<T> inTxFuture(
            Log log,
            ExecutorService pool,
            GraphDatabaseService db,
            Function<Transaction, T> function,
            long maxRetries,
            Consumer<Long> callbackForRetry,
            Consumer<Void> callbackAction) {
        try {
            return pool.submit(() -> {
                try {
                    return retryInTx(log, db, function, 0, maxRetries, callbackForRetry);
                } finally {
                    callbackAction.accept(null);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Error executing in separate transaction", e);
        }
    }

    public static <T> Future<T> inTxFuture(
            ExecutorService pool, GraphDatabaseService db, Function<Transaction, T> function) {
        return inTxFuture(null, pool, db, function, 0, _ignored -> {}, _ignored -> {});
    }

    public static <T> T inTx(GraphDatabaseService db, Pools pools, Function<Transaction, T> function) {
        try {
            return inTxFuture(pools.getDefaultExecutorService(), db, function).get();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            // TODO: consider dropping the exception to logs
            throw new RuntimeException("Error executing in separate transaction: " + e.getMessage(), e);
        }
    }

    public static <T> T inThread(Pools pools, Callable<T> callable) {
        try {
            return inFuture(pools, callable).get();
        } catch (Exception e) {
            throw new RuntimeException("Error executing in separate thread: " + e.getMessage(), e);
        }
    }

    public static <T> Future<T> inFuture(Pools pools, Callable<T> callable) {
        return pools.getDefaultExecutorService().submit(callable);
    }

    public static Double toDouble(Object value) {
        if (value == null) return null;
        if (value instanceof Number) return ((Number) value).doubleValue();
        try {
            return Double.parseDouble(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static Map<String, Object> subMap(Map<String, ?> params, String prefix) {
        Map<String, Object> config = new HashMap<>(10);
        int len = prefix.length() + (prefix.isEmpty() || prefix.endsWith(".") ? 0 : 1);
        for (Map.Entry<String, ?> entry : params.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(prefix)) {
                config.put(key.substring(len), entry.getValue());
            }
        }
        return config;
    }

    public static Long toLong(Object value) {
        if (value == null) return null;
        if (value instanceof Number) return ((Number) value).longValue();
        try {
            String s = value.toString();
            if (s.contains(".")) return (long) Double.parseDouble(s);
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static Integer toInteger(Object value) {
        if (value == null) return null;
        if (value instanceof Number) return ((Number) value).intValue();
        try {
            String s = value.toString();
            if (s.contains(".")) return (int) Double.parseDouble(s);
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static void writePayload(URLConnection con, String payload) throws IOException {
        if (payload == null) return;
        con.setDoOutput(true);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(con.getOutputStream(), "UTF-8"));
        writer.write(payload);
        writer.close();
    }

    public static StreamConnection getStreamConnection(
            String urlAddress, Map<String, Object> headers, String payload, URLAccessChecker urlAccessChecker)
            throws IOException, URISyntaxException, URLAccessValidationError {
        return FileUtils.getStreamConnection(
                FileUtils.from(urlAddress), urlAddress, headers, payload, urlAccessChecker);
    }

    public static boolean toBoolean(Object value) {
        if ((value == null
                || value instanceof Number && (((Number) value).longValue()) == 0L
                || value instanceof String
                        && (value.equals("")
                                || ((String) value).equalsIgnoreCase("false")
                                || ((String) value).equalsIgnoreCase("no")
                                || ((String) value).equalsIgnoreCase("0"))
                || value instanceof Boolean && value.equals(false))) {
            return false;
        }
        return true;
    }

    public static String toString(Object string) {
        return string == null ? null : string.toString();
    }

    public static String encodeUrlComponent(String value) {
        try {
            return URLEncoder.encode(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unsupported character set utf-8");
        }
    }

    public static String toJson(Object value) {
        try {
            return JsonUtil.OBJECT_MAPPER.writeValueAsString(value);
        } catch (IOException e) {
            throw new RuntimeException("Can't convert " + value + " to JSON");
        }
    }

    public static <T> T fromJson(String value, Class<T> type) {
        try {
            return JsonUtil.OBJECT_MAPPER.readValue(value, type);
        } catch (IOException e) {
            throw new RuntimeException("Can't convert " + value + " from JSON");
        }
    }

    public static Stream<List<Object>> partitionSubList(List<Object> data, int partitions) {
        return partitionSubList(data, partitions, null);
    }

    public static Stream<List<Object>> partitionSubList(List<Object> data, int partitions, List<Object> tombstone) {
        if (partitions == 0) partitions = 1;
        List<Object> list = new ArrayList<>(data);
        int total = list.size();
        int batchSize = Math.max((int) Math.ceil((double) total / partitions), 1);
        Stream<List<Object>> stream = IntStream.range(0, partitions)
                .parallel()
                .mapToObj((part) ->
                        list.subList(Math.min(part * batchSize, total), Math.min((part + 1) * batchSize, total)))
                .filter(partition -> !partition.isEmpty());
        return tombstone == null ? stream : Stream.concat(stream, Stream.of(tombstone));
    }

    public static Long runNumericQuery(Transaction tx, String query, Map<String, Object> params) {
        if (params == null) params = Collections.emptyMap();
        try (ResourceIterator<Long> it = tx.execute(query, params).<Long>columnAs("result")) {
            return it.next();
        }
    }

    public static long nodeCount(Transaction tx) {
        return runNumericQuery(tx, NODE_COUNT, null);
    }

    public static long relCount(Transaction tx) {
        return runNumericQuery(tx, REL_COUNT, null);
    }

    public static String readResourceFile(String name) {
        InputStream is = Util.class.getClassLoader().getResourceAsStream(name);
        return new Scanner(is).useDelimiter("\\Z").next();
    }

    public static Map<String, Object> merge(Map<String, Object> first, Map<String, Object> second) {
        if (second == null || second.isEmpty()) return first == null ? Collections.EMPTY_MAP : first;
        if (first == null || first.isEmpty()) return second == null ? Collections.EMPTY_MAP : second;
        Map<String, Object> combined = new HashMap<>(first);
        combined.putAll(second);
        return combined;
    }

    public static <T> Map<String, T> map(T... values) {
        Map<String, T> map = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            if (values[i] == null) continue;
            map.put(values[i].toString(), values[i + 1]);
        }
        return map;
    }

    public static <T> Map<String, T> map(List<T> pairs) {
        Map<String, T> res = new LinkedHashMap<>(pairs.size() / 2);
        Iterator<T> it = pairs.iterator();
        while (it.hasNext()) {
            Object key = it.next();
            T value = it.next();
            if (key != null) res.put(key.toString(), value);
        }
        return res;
    }

    public static <T, R> List<R> map(Stream<T> stream, Function<T, R> mapper) {
        return stream.map(mapper).collect(Collectors.toList());
    }

    public static <T, R> List<R> map(Collection<T> collection, Function<T, R> mapper) {
        return map(collection.stream(), mapper);
    }

    public static String cleanUrl(String url) {
        try {
            URL source = new URL(url);
            String file = source.getFile();
            if (source.getRef() != null) file += "#" + source.getRef();
            return new URL(source.getProtocol(), source.getHost(), source.getPort(), file).toString();
        } catch (MalformedURLException mfu) {
            return String.format("invalid URL (%s)", url);
        }
    }

    public static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            /* ignore */
        }
    }

    // This is in common, so I suspect this is used by extended, leaving as is, please use quoteIdentifierSafely
    // instead.
    @Deprecated
    public static String quote(String var) {
        return SourceVersion.isIdentifier(var) && !var.contains("$") ? var : '`' + var + '`';
    }

    public static String quoteIdentifierSafely(String value) {
        if (SourceVersion.isIdentifier(value) && !value.contains("$")) {
            return value;
        }
        return sanitize(value, true);
    }

    private static final String ESCAPED_UNICODE_BACKTICK = "\\u0060";

    private static final Pattern PATTERN_ESCAPED_4DIGIT_UNICODE = Pattern.compile("\\\\u+(\\p{XDigit}{4})");
    private static final Pattern PATTERN_LABEL_AND_TYPE_QUOTATION = Pattern.compile("(?<!`)`(?:`{2})*(?!`)");

    private static final List<String[]> SUPPORTED_ESCAPE_CHARS = Collections.unmodifiableList(Arrays.asList(
            new String[] {"\\b", "\b"},
            new String[] {"\\f", "\f"},
            new String[] {"\\n", "\n"},
            new String[] {"\\r", "\r"},
            new String[] {"\\t", "\t"},
            new String[] {"\\`", "``"}));

    /**
     * Sanitizes the given input to be used as a valid schema name
     *
     * @param value The value to sanitize
     * @return A value that is safe to be used in string concatenation, an empty optional indicates a value that cannot be safely quoted
     */
    public static String sanitize(String value) {
        return sanitize(value, false);
    }

    /**
     * Sanitizes the given input to be used as a valid schema name
     *
     * @param value The value to sanitize
     * @param addQuotes If quotation should be added
     * @return A value that is safe to be used in string concatenation, an empty optional indicates a value that cannot be safely quoted
     */
    public static String sanitize(String value, boolean addQuotes) {
        if (value == null || value.isEmpty()) {
            return value;
        }

        // Replace escaped chars
        for (String[] pair : SUPPORTED_ESCAPE_CHARS) {
            value = value.replace(pair[0], pair[1]);
        }
        value = value.replace(ESCAPED_UNICODE_BACKTICK, "`");

        // Replace escaped octal hex
        // Excluding the support for 6 digit literals, as this contradicts the overall example in CIP-59r
        Matcher matcher = PATTERN_ESCAPED_4DIGIT_UNICODE.matcher(value);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String replacement = Character.toString((char) Integer.parseInt(matcher.group(1), 16));
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }

        matcher.appendTail(sb);
        value = sb.toString();

        value = value.replace("\\u", "\\u005C\\u0075");

        matcher = PATTERN_LABEL_AND_TYPE_QUOTATION.matcher(value);
        value = matcher.replaceAll("`$0");
        value = value.replace("\\\\", "\\");

        if (!addQuotes) {
            return value;
        }

        return String.format(Locale.ENGLISH, "`%s`", value);
    }

    public static String param(String var) {
        return var.charAt(0) == '$' ? var : '$' + quoteIdentifierSafely(var);
    }

    public static String withMapping(Stream<String> columns, Function<String, String> withMapping) {
        String with = columns.map(withMapping).collect(Collectors.joining(","));
        return with.isEmpty() ? with : " WITH " + with + " ";
    }

    public static boolean isWriteableInstance(GraphDatabaseAPI db, Transaction systemTx) {
        var socketAddress = db.getDependencyResolver()
                .resolveDependency(Config.class)
                .get(BoltConnector.advertised_address)
                .toString();
        String query = "SHOW DATABASE $databaseName WHERE address = $socketAddress";
        final Map<String, Object> params = Map.of("databaseName", db.databaseName(), "socketAddress", socketAddress);
        if (systemTx == null) {
            GraphDatabaseService systemDb = db.getDependencyResolver()
                    .resolveDependency(DatabaseManagementService.class)
                    .database(SYSTEM_DATABASE_NAME);
            return systemDb.executeTransactionally(query, params, Util::getSingleWriter);
        } else {
            final Result result = systemTx.execute(query, params);
            return getSingleWriter(result);
        }
    }

    public static boolean isWriteableInstance(GraphDatabaseAPI db) {
        return isWriteableInstance(db, null);
    }

    private static Boolean getSingleWriter(Result result) {
        return Iterators.single(result.columnAs("writer"));
    }

    /**
     * Given a context related to the procedure invocation this method checks if the transaction is terminated in some way
     *
     * @param db
     * @return
     *
     */
    public static boolean transactionIsTerminated(TerminationGuard db) {
        try {
            db.check();
            return false;
        } catch (TransactionTerminatedException | NotInTransactionException tge) {
            return true;
        }
    }

    public static void close(AutoCloseable closeable, Consumer<Exception> onError) {
        try {
            if (closeable != null) closeable.close();
        } catch (Exception e) {
            // Consume the exception if requested, else ignore
            if (onError != null) {
                onError.accept(e);
            }
        }
    }

    public static void close(AutoCloseable closeable) {
        close(closeable, null);
    }

    public static boolean classExists(String className) {
        try {
            Class.forName(className);
            return true;
        } catch (Throwable e) {
            if (PRINT_CLASS_CHECK_STACK_TRACES) {
                e.printStackTrace();
            }
            return false;
        }
    }

    public static Optional<String> getLoadUrlByConfigFile(String loadType, String key, String suffix) {
        key = Optional.ofNullable(key)
                .map(s -> Stream.of("apoc", loadType, s, suffix).collect(Collectors.joining(".")))
                .orElse(StringUtils.EMPTY);
        String value = apocConfig().getString(key);
        return Optional.ofNullable(value);
    }

    public static DateTimeFormatter getFormat(String format) {
        return getOrCreate(format);
    }

    public static char parseCharFromConfig(Map<String, Object> config, String key, char defaultValue) {
        String separator = (String) config.getOrDefault(key, "");
        if (separator == null || separator.isEmpty()) {
            return defaultValue;
        }
        if ("TAB".equals(separator)) {
            return '\t';
        }
        // "NONE" is used to resolve cases like issue #1376.
        // That is, when I have a line like "VER: AX\GEARBOX\ASSEMBLY" and I don't want to convert it in "VER:
        // AXGEARBOXASSEMBLY"
        if ("NONE".equals(separator)) {
            return '\0';
        }
        return separator.charAt(0);
    }

    public static Map<String, Object> flattenMap(Map<String, Object> map, String prefix) {
        return map.entrySet().stream()
                .flatMap(entry -> {
                    String key;
                    if (prefix != null && !prefix.isEmpty()) {
                        key = prefix + "." + entry.getKey();
                    } else {
                        key = entry.getKey();
                    }
                    Object value = entry.getValue();
                    if (value instanceof Map) {
                        return flattenMap((Map<String, Object>) value, key).entrySet().stream();
                    } else {
                        Map.Entry<String, Object> newEntry = new AbstractMap.SimpleEntry(key, entry.getValue());
                        return Stream.of(newEntry);
                    }
                })
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    }

    public static Node rebind(Transaction tx, Node node) {
        return node instanceof VirtualNode ? node : tx.getNodeByElementId(node.getElementId());
    }

    public static Relationship rebind(Transaction tx, Relationship rel) {
        return rel instanceof VirtualRelationship ? rel : tx.getRelationshipByElementId(rel.getElementId());
    }

    public static <T extends Entity> T rebind(Transaction tx, T e) {
        if (e instanceof Node) {
            return (T) rebind(tx, (Node) e);
        } else {
            return (T) rebind(tx, (Relationship) e);
        }
    }

    public static <T extends Entity> List<T> rebind(List<T> entities, Transaction tx) {
        return entities.stream().map(n -> Util.rebind(tx, n)).collect(Collectors.toList());
    }

    public static Node mergeNode(
            Transaction tx, Label primaryLabel, Label addtionalLabel, Pair<String, Object>... pairs) {
        Node node;
        try (ResourceIterator<Node> nodes = tx.findNodes(primaryLabel, pairs[0].getLeft(), pairs[0].getRight())) {
            node = Iterators.singleOrNull(nodes.stream()
                    .filter(n -> addtionalLabel == null || n.hasLabel(addtionalLabel))
                    .filter(n -> {
                        for (int i = 1; i < pairs.length; i++) {
                            if (!Objects.deepEquals(pairs[i].getRight(), n.getProperty(pairs[i].getLeft(), null))) {
                                return false;
                            }
                        }
                        return true;
                    })
                    .iterator());
        }
        if (node == null) {
            Label[] labels =
                    addtionalLabel == null ? new Label[] {primaryLabel} : new Label[] {primaryLabel, addtionalLabel};
            node = tx.createNode(labels);
            for (int i = 0; i < pairs.length; i++) {
                node.setProperty(pairs[i].getLeft(), pairs[i].getRight());
            }
        }
        return node;
    }

    public static void validateQuery(
            GraphDatabaseService db, String statement, QueryExecutionType.QueryType... supportedQueryTypes) {
        validateQuery(db, statement, Collections.emptySet(), supportedQueryTypes);
    }

    public static void validateQuery(
            GraphDatabaseService db,
            String statement,
            Set<Mode> supportedModes,
            QueryExecutionType.QueryType... supportedQueryTypes) {
        db.executeTransactionally("EXPLAIN " + statement, Collections.emptyMap(), result -> {
            final boolean isQueryTypeValid = supportedQueryTypes == null
                    || supportedQueryTypes.length == 0
                    || Stream.of(supportedQueryTypes)
                            .anyMatch(sqt ->
                                    sqt.equals(result.getQueryExecutionType().queryType()));

            if (!isQueryTypeValid) {
                throw new RuntimeException(
                        "Supported query types for the operation are " + Arrays.toString(supportedQueryTypes));
            }

            if (!procsAreValid(db, supportedModes, result)) {
                throw new RuntimeException(
                        "Supported inner procedure modes for the operation are " + new TreeSet<>(supportedModes));
            }

            return null;
        });
    }

    private static boolean procsAreValid(GraphDatabaseService db, Set<Mode> supportedModes, Result result) {
        if (supportedModes != null && !supportedModes.isEmpty()) {
            final ExecutionPlanDescription executionPlanDescription = result.getExecutionPlanDescription();
            // get procedures used in the query
            Set<String> queryProcNames = new HashSet<>();
            getAllQueryProcs(executionPlanDescription, queryProcNames);

            if (!queryProcNames.isEmpty()) {
                final Set<String> modes =
                        supportedModes.stream().map(Mode::name).collect(Collectors.toSet());
                // check if sub-procedures have valid mode
                final Set<String> procNames = db.executeTransactionally(
                        "SHOW PROCEDURES YIELD name, mode where mode in $modes return name",
                        Map.of("modes", modes),
                        r -> Iterators.asSet(r.columnAs("name")));

                return procNames.containsAll(queryProcNames);
            }
        }

        return true;
    }

    public static void getAllQueryProcs(ExecutionPlanDescription executionPlanDescription, Set<String> procs) {
        executionPlanDescription.getChildren().forEach(i -> {
            // if executionPlanDescription is a ProcedureCall
            // we return proc. name from "Details"
            // by extracting up to the first `(` char, e.g. apoc.schema.assert(null, null)....
            if (i.getName().equals("ProcedureCall")) {
                final String procName = ((String) i.getArguments().get("Details")).split("\\(")[0];
                procs.add(procName);
            }
            getAllQueryProcs(i, procs);
        });
    }

    /**
     * all new threads being created within apoc should be daemon threads to allow graceful termination, so use this whenever you need a thread
     * @param target
     * @return
     */
    public static Thread newDaemonThread(Runnable target) {
        Thread thread = new Thread(target);
        thread.setDaemon(true);
        return thread;
    }

    public static String encodeUserColonPassToBase64(String userPass) {
        return new String(Base64.getEncoder().encode((userPass).getBytes()));
    }

    public static Map<String, Object> extractCredentialsIfNeeded(String url, boolean failOnError) {
        try {
            URI uri = new URI(encodePath(url));
            String authInfo = uri.getUserInfo();
            if (null != authInfo) {
                String[] parts = authInfo.split(":");
                if (2 == parts.length) {
                    String token = encodeUserColonPassToBase64(authInfo);
                    return MapUtil.map("Authorization", "Basic " + token);
                }
            }
        } catch (Exception e) {
            if (!failOnError) return Collections.emptyMap();
            else throw new RuntimeException(e);
        }

        return Collections.emptyMap();
    }

    public static PointValue toPoint(Map<String, Object> pointMap, Map<String, Object> defaultPointMap) {
        double x;
        double y;
        Double z = null;

        String givenCRS = (String) getOrDefault(pointMap, defaultPointMap, "crs");
        if (givenCRS == null) {
            if (pointMap.containsKey("latitude") && pointMap.containsKey("longitude")) {
                // WGS-84-3D is assumed; docs say height or x is allowed
                if (pointMap.containsKey("height") || pointMap.containsKey("x")) {
                    givenCRS = CoordinateReferenceSystem.WGS_84_3D.getName();
                } else {
                    givenCRS = CoordinateReferenceSystem.WGS_84.getName();
                }
            }

            if (pointMap.containsKey("x") && pointMap.containsKey("y")) {
                if (pointMap.containsKey("z")) {
                    givenCRS = CoordinateReferenceSystem.CARTESIAN_3D.getName();
                } else {
                    givenCRS = CoordinateReferenceSystem.CARTESIAN.getName();
                }
            }
        }

        if (givenCRS == null) {
            // This means the user gave no CRS, and we were unable to infer it which means it is an invalid point
            throw new RuntimeException("Cannot convert the map with keys: " + pointMap.keySet() + " to a POINT value.");
        }

        final CoordinateReferenceSystem crs = CoordinateReferenceSystem.byName(givenCRS);

        // It does not depend on the prefix of crs, I could also pass a point({x: 56.7, y: 12.78, crs: 'wgs-84'})
        final boolean isLatitudePresent = pointMap.containsKey("latitude")
                || (!pointMap.containsKey("x") && defaultPointMap.containsKey("latitude"));
        final boolean isCoord3D = crs.getName().endsWith("-3d");
        if (isLatitudePresent) {
            x = Util.toDouble(getOrDefault(pointMap, defaultPointMap, "longitude"));
            y = Util.toDouble(getOrDefault(pointMap, defaultPointMap, "latitude"));
            if (isCoord3D) {
                z = Util.toDouble(getOrDefault(pointMap, defaultPointMap, "height"));
            }
        } else {
            x = Util.toDouble(getOrDefault(pointMap, defaultPointMap, "x"));
            y = Util.toDouble(getOrDefault(pointMap, defaultPointMap, "y"));
            if (isCoord3D) {
                z = Util.toDouble(getOrDefault(pointMap, defaultPointMap, "z"));
            }
        }

        return z != null ? Values.pointValue(crs, x, y, z) : Values.pointValue(crs, x, y);
    }

    private static Object getOrDefault(Map<String, Object> firstMap, Map<String, Object> secondMap, String key) {
        return firstMap.getOrDefault(key, secondMap.get(key));
    }

    public static Object getStringOrCompressedData(StringWriter writer, ExportConfig config) {
        try {
            final String compression = config.getCompressionAlgo();
            final String writerString = writer.toString();
            Object data = compression.equals(CompressionAlgo.NONE.name())
                    ? writerString
                    : CompressionAlgo.valueOf(compression).compress(writerString, config.getCharset());
            writer.getBuffer().setLength(0);
            return data;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public enum ConstraintCategory {
        RELATIONSHIP,
        NODE
    }

    public static ConstraintCategory getConstraintCategory(ConstraintType type) {
        return switch (type) {
            case NODE_KEY,
                    NODE_PROPERTY_EXISTENCE,
                    UNIQUENESS,
                    NODE_PROPERTY_TYPE,
                    NODE_LABEL_EXISTENCE -> ConstraintCategory.NODE;
            case RELATIONSHIP_KEY,
                    RELATIONSHIP_UNIQUENESS,
                    RELATIONSHIP_PROPERTY_EXISTENCE,
                    RELATIONSHIP_PROPERTY_TYPE,
                    RELATIONSHIP_SOURCE_LABEL,
                    RELATIONSHIP_TARGET_LABEL -> ConstraintCategory.RELATIONSHIP;
            default -> throw new IllegalStateException("Constraint with a type not supported by apoc");
        };
    }

    public static ConstraintCategory getConstraintCategory(ConstraintDescriptor descriptor) {
        if (descriptor.isNodeUniquenessConstraint()
                || descriptor.isNodePropertyExistenceConstraint()
                || descriptor.isNodeKeyConstraint()
                || descriptor.isNodePropertyTypeConstraint()
                || descriptor.isNodeLabelExistenceConstraint()) {
            return ConstraintCategory.NODE;
        }
        if (descriptor.isRelationshipKeyConstraint()
                || descriptor.isRelationshipPropertyExistenceConstraint()
                || descriptor.isRelationshipUniquenessConstraint()
                || descriptor.isRelationshipPropertyTypeConstraint()
                || descriptor.isRelationshipEndpointLabelConstraint()) {
            return ConstraintCategory.RELATIONSHIP;
        }
        return ConstraintCategory.NODE;
    }

    public static boolean isNodeCategory(ConstraintType type) {
        return getConstraintCategory(type) == ConstraintCategory.NODE;
    }

    public static boolean isRelationshipCategory(ConstraintType type) {
        return getConstraintCategory(type) == ConstraintCategory.RELATIONSHIP;
    }

    public static boolean isNodeCategory(ConstraintDescriptor descriptor) {
        return getConstraintCategory(descriptor) == ConstraintCategory.NODE;
    }

    public static boolean isRelationshipCategory(ConstraintDescriptor descriptor) {
        return getConstraintCategory(descriptor) == ConstraintCategory.RELATIONSHIP;
    }

    public static Iterable<IndexDefinition> getIndexes(Transaction transaction) {
        return transaction.schema().getIndexes();
    }

    public static Iterable<IndexDefinition> getIndexes(Transaction transaction, Label label) {
        return transaction.schema().getIndexes(label);
    }

    public static Iterable<IndexDefinition> getIndexes(Transaction transaction, RelationshipType relType) {
        return transaction.schema().getIndexes(relType);
    }

    public static String getCypherVersionString(ProcedureCallContext procedureCallContext) {
        if (procedureCallContext.calledwithQueryLanguage().equals(QueryLanguage.CYPHER_5)) {
            return "5";
        } else if (procedureCallContext.calledwithQueryLanguage().equals(QueryLanguage.CYPHER_25)) {
            return "25";
        } else {
            // When a new version of Cypher is added, please update here :)
            throw new RuntimeException(
                    "Unsupported cypher query language: " + procedureCallContext.calledwithQueryLanguage());
        }
    }

    private static final Pattern CYPHER_VERSION_PATTERN =
            Pattern.compile("^\\s*\\b(CYPHER)(?:\\s+(\\d+))?", Pattern.CASE_INSENSITIVE);

    public static String prefixQueryWithCheck(ProcedureCallContext procedureCallContext, String query) {
        return prefixQueryWithCheck(getCypherVersionString(procedureCallContext), query);
    }

    /**
     * A helper function to prefix a query with a Cypher Version; if it is not already prefixed.
     * @param cypherVersion a string of the version number
     * @return The prefixed query
     */
    public static String prefixQueryWithCheck(String cypherVersion, String query) {
        List<String> cypherPrefix = extractCypherPrefix(query, cypherVersion);
        if (Objects.equals(cypherPrefix.getFirst(), "")) {
            return cypherPrefix.get(1) + " " + query;
        }
        return query.replaceFirst("(?i)" + cypherPrefix.getFirst(), cypherPrefix.get(1) + " ");
    }

    // Extract the Cypher prefix, add version if missing
    // This will return a list of 2 items, the first being what was extracted (empty string if nothing)
    // The second being what the prefix should be.
    public static List<String> extractCypherPrefix(String input, String cypherVersion) {
        String cypherVersionPrefix = "CYPHER " + cypherVersion;
        Matcher matcher = CYPHER_VERSION_PATTERN.matcher(input);
        if (matcher.find()) {
            String cypher = matcher.group(1); // Always "CYPHER"
            String version = matcher.group(2); // Optional version
            return List.of(
                    version != null ? cypher + " " + version : cypher,
                    version != null ? cypher + " " + version : cypherVersionPrefix);
        }
        return List.of("", cypherVersionPrefix); // No prefix was found
    }

    public static int size(Iterable<?> iterable) {
        if (iterable instanceof Collection<?> c) return c.size();
        else
            return Math.toIntExact(
                    StreamSupport.stream(iterable.spliterator(), false).count());
    }

    public static <T> boolean contains(Iterable<? extends T> iterable, T value) {
        if (iterable instanceof Collection<?> c) return c.contains(value);
        else return StreamSupport.stream(iterable.spliterator(), false).anyMatch(v -> Objects.equals(value, v));
    }
}
