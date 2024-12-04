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
package apoc.util;

import apoc.PoolsExtended;
import apoc.convert.ConvertUtilsExtended;
import apoc.export.util.CountingInputStreamExtended;
import apoc.export.util.ExportConfigExtended;
import apoc.result.VirtualNodeExtended;
import apoc.result.VirtualRelationshipExtended;
import apoc.util.collection.IteratorsExtended;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.io.IOUtils;
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
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.TerminationGuard;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import javax.lang.model.SourceVersion;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.net.HttpURLConnection;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.ExtendedApocConfig.extendedApocConfig;
import static apoc.export.util.LimitedSizeInputStreamExtended.toLimitedIStream;
import static apoc.util.DateFormatUtilExtended.getOrCreate;
import static java.net.HttpURLConnection.HTTP_NOT_MODIFIED;
import static org.eclipse.jetty.util.URIUtil.encodePath;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

/**
 * @author mh
 * @since 24.04.16
 */
public class UtilExtended {

    public static final Label[] NO_LABELS = new Label[0];
    public static final String NODE_COUNT = "MATCH (n) RETURN count(*) as result";
    public static final String REL_COUNT = "MATCH ()-->() RETURN count(*) as result";
    public static final String ERROR_BYTES_OR_STRING = "Only byte[] or url String allowed";

    public static final int REDIRECT_LIMIT = 10;

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

    public static Label[] labels(Object labelNames) {
        if (labelNames == null) return NO_LABELS;
        if (labelNames instanceof List) {
            Set names = new LinkedHashSet((List) labelNames); // Removing duplicates
            Label[] labels = new Label[names.size()];
            int i = 0;
            for (Object l : names) {
                if (l == null) continue;
                labels[i++] = Label.label(l.toString());
            }
            if (i <= labels.length) return Arrays.copyOf(labels, i);
            return labels;
        }
        return new Label[] {Label.label(labelNames.toString())};
    }

    public static Label[] getLabelsArray(Node node) {
        List<Label> labels = getLabels(node);
        return labels.toArray(new Label[0]);
    }

    public static List<Label> getLabels(Node node) {
        return getLabelsAsStream(node).collect(Collectors.toList());
    }

    private static Stream<Label> getLabelsAsStream(Node node) {
        return StreamSupport.stream(node.getLabels().spliterator(), false);
    }

    public static Stream<Object> stream(Object values) {
        return ConvertUtilsExtended.convertToList(values).stream();
    }

    public static Stream<Node> nodeStream(InternalTransaction tx, Object ids) {
        return stream(ids).map(id -> node(tx, id));
    }

    public static List<Node> nodeList(InternalTransaction tx, Object ids) {
        if (ids == null) return List.of();
        return stream(ids).map(id -> node(tx, id)).toList();
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
            UtilExtended.sleep(100);
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

    public static <T> T inTx(GraphDatabaseService db, PoolsExtended pools, Function<Transaction, T> function) {
        try {
            return inTxFuture(pools.getDefaultExecutorService(), db, function).get();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            // TODO: consider dropping the exception to logs
            throw new RuntimeException("Error executing in separate transaction: " + e.getMessage(), e);
        }
    }

    public static <T> T inThread(PoolsExtended pools, Callable<T> callable) {
        try {
            return inFuture(pools, callable).get();
        } catch (Exception e) {
            throw new RuntimeException("Error executing in separate thread: " + e.getMessage(), e);
        }
    }

    public static <T> Future<T> inFuture(PoolsExtended pools, Callable<T> callable) {
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

    public static URLConnection openUrlConnection(URL src, Map<String, Object> headers) throws IOException {
        URLConnection con = src.openConnection();
        con.setRequestProperty("User-Agent", "APOC Procedures for Neo4j");
        if (con instanceof HttpURLConnection) {
            HttpURLConnection http = (HttpURLConnection) con;
            http.setInstanceFollowRedirects(false);
            if (headers != null) {
                Object method = headers.get("method");
                if (method != null) {
                    http.setRequestMethod(method.toString());
                    http.setChunkedStreamingMode(1024 * 1024);
                }
                headers.forEach((k, v) -> con.setRequestProperty(k, v == null ? "" : v.toString()));
            }
        }

        con.setConnectTimeout(extendedApocConfig().getInt("apoc.http.timeout.connect", 10_000));
        con.setReadTimeout(extendedApocConfig().getInt("apoc.http.timeout.read", 60_000));
        return con;
    }

    public static boolean isRedirect(HttpURLConnection con) throws IOException {
        int responseCode = con.getResponseCode();
        boolean isRedirectCode =
                responseCode >= 300 && responseCode <= 307 && responseCode != 306 && responseCode != HTTP_NOT_MODIFIED;
        if (isRedirectCode) {
            URL location = new URL(con.getHeaderField("Location"));
            String oldProtocol = con.getURL().getProtocol();
            String protocol = location.getProtocol();
            if (!protocol.equals(oldProtocol)
                    && !protocol.startsWith(oldProtocol)) { // we allow http -> https redirect and similar
                throw new RuntimeException("The redirect URI has a different protocol: " + location.toString());
            }
        }
        return isRedirectCode;
    }

    private static void writePayload(URLConnection con, String payload) throws IOException {
        if (payload == null) return;
        con.setDoOutput(true);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(con.getOutputStream(), "UTF-8"));
        writer.write(payload);
        writer.close();
    }

    private static String handleRedirect(URLConnection con, String url) throws IOException {
        if (!(con instanceof HttpURLConnection)) return url;
        if (!isRedirect(((HttpURLConnection) con))) return url;
        return con.getHeaderField("Location");
    }

    public static CountingInputStreamExtended openInputStream(
            Object input,
            Map<String, Object> headers,
            String payload,
            String compressionAlgo,
            URLAccessChecker urlAccessChecker)
            throws IOException, URISyntaxException, URLAccessValidationError {
        if (input instanceof String) {
            String urlAddress = (String) input;
            final ArchiveTypeExtended archiveType = ArchiveTypeExtended.from(urlAddress);
            if (archiveType.isArchive()) {
                return getStreamCompressedFile(urlAddress, headers, payload, archiveType, urlAccessChecker);
            }

            StreamConnectionExtended sc = getStreamConnection(urlAddress, headers, payload, urlAccessChecker);
            return sc.toCountingInputStream(compressionAlgo);
        } else if (input instanceof byte[]) {
            return FileUtilsExtended.getInputStreamFromBinary((byte[]) input, compressionAlgo);
        } else {
            throw new RuntimeException(ERROR_BYTES_OR_STRING);
        }
    }

    private static CountingInputStreamExtended getStreamCompressedFile(
            String urlAddress,
            Map<String, Object> headers,
            String payload,
            ArchiveTypeExtended archiveType,
            URLAccessChecker urlAccessChecker)
            throws IOException, URISyntaxException, URLAccessValidationError {
        StreamConnectionExtended sc;
        InputStream stream;
        String[] tokens = urlAddress.split("!");
        urlAddress = tokens[0];
        String zipFileName;
        if (tokens.length == 2) {
            zipFileName = tokens[1];
            sc = getStreamConnection(urlAddress, headers, payload, urlAccessChecker);
            stream = getFileStreamIntoCompressedFile(sc.getInputStream(), zipFileName, archiveType);
            stream = toLimitedIStream(stream, sc.getLength());
        } else throw new IllegalArgumentException("filename can't be null or empty");

        return new CountingInputStreamExtended(stream, sc.getLength());
    }

    public static StreamConnectionExtended getStreamConnection(
            String urlAddress, Map<String, Object> headers, String payload, URLAccessChecker urlAccessChecker)
            throws IOException, URISyntaxException, URLAccessValidationError {
        return FileUtilsExtended.getStreamConnection(
                FileUtilsExtended.from(urlAddress), urlAddress, headers, payload, urlAccessChecker);
    }

    private static InputStream getFileStreamIntoCompressedFile(InputStream is, String fileName, ArchiveTypeExtended archiveType)
            throws IOException {
        try (ArchiveInputStream archive = archiveType.getInputStream(is)) {
            ArchiveEntry archiveEntry;

            while ((archiveEntry = archive.getNextEntry()) != null) {
                if (!archiveEntry.isDirectory() && archiveEntry.getName().equals(fileName)) {
                    return new ByteArrayInputStream(IOUtils.toByteArray(archive));
                }
            }
        }

        return null;
    }

    public static StreamConnectionExtended readHttpInputStream(
            String urlAddress,
            Map<String, Object> headers,
            String payload,
            int redirectLimit,
            URLAccessChecker urlAccessChecker)
            throws IOException {
        URL url = extendedApocConfig().checkAllowedUrlAndPinToIP(urlAddress, urlAccessChecker);
        URLConnection con = openUrlConnection(url, headers);
        writePayload(con, payload);
        String newUrl = handleRedirect(con, urlAddress);
        if (newUrl != null && !urlAddress.equals(newUrl)) {
            con.getInputStream().close();
            if (redirectLimit == 0) {
                throw new IOException("Redirect limit exceeded");
            }
            return readHttpInputStream(newUrl, headers, payload, --redirectLimit, urlAccessChecker);
        }

        return new StreamConnectionExtended.UrlStreamConnection(con);
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
            return JsonUtilExtended.OBJECT_MAPPER.writeValueAsString(value);
        } catch (IOException e) {
            throw new RuntimeException("Can't convert " + value + " to JSON");
        }
    }

    public static <T> T fromJson(String value, Class<T> type) {
        try {
            return JsonUtilExtended.OBJECT_MAPPER.readValue(value, type);
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
        InputStream is = UtilExtended.class.getClassLoader().getResourceAsStream(name);
        return new Scanner(is).useDelimiter("\\Z").next();
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> readMap(String value) {
        try {
            return JsonUtilExtended.OBJECT_MAPPER.readValue(value, Map.class);
        } catch (IOException e) {
            throw new RuntimeException("Couldn't read as JSON " + value);
        }
    }

    public static <T> List<T> take(Iterator<T> iterator, int batchsize) {
        List<T> result = new ArrayList<>(batchsize);
        while (iterator.hasNext() && batchsize-- > 0) {
            result.add(iterator.next());
        }
        return result;
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

    public static Map<String, Object> mapFromLists(List<String> keys, List<Object> values) {
        if (keys == null || values == null || keys.size() != values.size())
            throw new RuntimeException("keys and values lists have to be not null and of same size");
        if (keys.isEmpty()) return Collections.emptyMap();
        if (keys.size() == 1) return Collections.singletonMap(keys.get(0), values.get(0));
        ListIterator<Object> it = values.listIterator();
        Map<String, Object> res = new LinkedHashMap<>(keys.size());
        for (String key : keys) {
            res.put(key, it.next());
        }
        return res;
    }

    public static Map<String, Object> mapFromPairs(List<List<Object>> pairs) {
        if (pairs.isEmpty()) return Collections.emptyMap();
        Map<String, Object> map = new LinkedHashMap<>(pairs.size());
        for (List<Object> pair : pairs) {
            if (pair.isEmpty()) continue;
            Object key = pair.get(0);
            if (key == null) continue;
            Object value = pair.size() >= 2 ? pair.get(1) : null;
            map.put(key.toString(), value);
        }
        return map;
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

    public static <T> T getFuture(Future<T> f, Map<String, Long> errorMessages, AtomicInteger errors, T errorValue) {
        try {
            T t = f.get();
            return t;
        } catch (Exception e) {
            errors.incrementAndGet();
            errorMessages.compute(e.getMessage(), (s, i) -> i == null ? 1 : i + 1);
            return errorValue;
        }
    }

    public static <T> T getFutureOrCancel(
            Future<T> f, Map<String, Long> errorMessages, AtomicInteger errors, T errorValue) {
        try {
            if (f.isDone()) return f.get();
            else {
                f.cancel(false);
                errors.incrementAndGet();
            }
        } catch (Exception e) {
            errors.incrementAndGet();
            errorMessages.compute(e.getMessage(), (s, i) -> i == null ? 1 : i + 1);
        }
        return errorValue;
    }

    public static void logErrors(String message, Map<String, Long> errors, Log log) {
        if (!errors.isEmpty()) {
            log.warn(message);
            errors.forEach((k, v) -> log.warn("%d times: %s", v, k));
        }
    }

    public static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            /* ignore */
        }
    }

    public static String quote(String var) {
        return SourceVersion.isIdentifier(var) && !var.contains("$") ? var : '`' + var + '`';
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
        return var.charAt(0) == '$' ? var : '$' + quote(var);
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
            return systemDb.executeTransactionally(query, params, UtilExtended::getSingleWriter);
        } else {
            final Result result = systemTx.execute(query, params);
            return getSingleWriter(result);
        }
    }

    public static boolean isWriteableInstance(GraphDatabaseAPI db) {
        return isWriteableInstance(db, null);
    }

    private static Boolean getSingleWriter(Result result) {
        return IteratorsExtended.single(result.columnAs("writer"));
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

    public static void waitForFutures(List<Future> futures) {
        for (Future future : futures) {
            try {
                if (future != null) future.get();
            } catch (InterruptedException | ExecutionException e) {
                // ignore
            }
        }
    }

    public static void removeFinished(List<Future> futures) {
        if (futures.size() > 25) {
            futures.removeIf(Future::isDone);
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

    public static boolean isNotNullOrEmpty(String s) {
        return s != null && s.trim().length() != 0;
    }

    public static boolean isNullOrEmpty(String s) {
        return s == null || s.trim().length() == 0;
    }

    public static Map<String, String> getRequestParameter(String parameters) {
        Map<String, String> params = null;

        if (Objects.nonNull(parameters)) {
            params = new HashMap<>();
            String[] queryStrings = parameters.split("&");
            for (String query : queryStrings) {
                String[] parts = query.split("=");
                if (parts.length == 2) {
                    params.put(parts[0], parts[1]);
                }
            }
        }
        return params;
    }

    public static boolean classExists(String className) {
        try {
            Class.forName(className);
            return true;
        } catch (ClassNotFoundException cnfe) {
            return false;
        }
    }

    public static <T> T createInstanceOrNull(String className) {
        try {
            return (T) Class.forName(className).getDeclaredConstructor().newInstance();
        } catch (IllegalAccessException
                | InstantiationException
                | ClassNotFoundException
                | NoSuchMethodException
                | InvocationTargetException e) {
            return null;
        }
    }

    public static Optional<String> getLoadUrlByConfigFile(String loadType, String key, String suffix) {
        key = Optional.ofNullable(key)
                .map(s -> Stream.of("apoc", loadType, s, suffix).collect(Collectors.joining(".")))
                .orElse(StringUtils.EMPTY);
        String value = extendedApocConfig().getString(key);
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
        if (node instanceof VirtualNodeExtended) {
            return node;
        } else if (node.getId() < 0) {
            return node;
        } else if (node.getClass().getCanonicalName().equals("VirtualNode")) {
            return node;
        } else {
            return tx.getNodeByElementId(node.getElementId());
        }
    }

    public static Relationship rebind(Transaction tx, Relationship rel) {
        if (rel instanceof VirtualRelationshipExtended) {
            return rel;
        } else if (rel.getId() < 0) {
            return rel;
        } else if (rel.getClass().getCanonicalName().equals("VirtualRelationship")) {
            return rel;
        } else {
            return tx.getRelationshipByElementId(rel.getElementId());   
        }
    }

    public static <T extends Entity> T rebind(Transaction tx, T e) {
        if (e instanceof Node) {
            return (T) rebind(tx, (Node) e);
        } else {
            return (T) rebind(tx, (Relationship) e);
        }
    }

    public static <T extends Entity> List<T> rebind(List<T> entities, Transaction tx) {
        return entities.stream().map(n -> UtilExtended.rebind(tx, n)).collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    public static List<Map<String, Object>> rebindRows(Transaction tx, List<Map<String, Object>> rows) {
        for (var row : rows) if (needsRebind(row)) return (List<Map<String, Object>>) recursiveRebind(tx, rows);
        return rows;
    }

    private static Object recursiveRebind(Transaction tx, Object o) {
        if (o instanceof Entity entity) {
            return rebind(tx, entity);
        } else if (o instanceof Map<?, ?> map && needsRebind(map)) {
            Map<String, Object> m = new HashMap<>();
            map.forEach((key, value) -> m.put(key.toString(), recursiveRebind(tx, value)));
            return m;
        } else if (o instanceof List<?> list && needsRebind(list)) {
            return list.stream().map(e -> recursiveRebind(tx, e)).toList();
        } else {
            return o;
        }
    }

    private static boolean needsRebind(Map<?, ?> map) {
        return needsRebind(map.values());
    }

    private static boolean needsRebind(Collection<?> values) {
        for (var value : values) {
            if (value instanceof Map || value instanceof List || value instanceof Entity) {
                return true;
            }
        }
        return false;
    }

    public static Node mergeNode(
            Transaction tx, Label primaryLabel, Label addtionalLabel, Pair<String, Object>... pairs) {
        Node node = IteratorsExtended.singleOrNull(tx.findNodes(primaryLabel, pairs[0].getLeft(), pairs[0].getRight()).stream()
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

    public static <T> Set<T> intersection(Collection<T> a, Collection<T> b) {
        if (a == null || b == null) {
            return Collections.emptySet();
        }
        Set<T> intersection = new HashSet<>(a);
        intersection.retainAll(b);
        return intersection;
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
                        r -> IteratorsExtended.asSet(r.columnAs("name")));

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
                    return MapUtilExtended.map("Authorization", "Basic " + token);
                }
            }
        } catch (Exception e) {
            if (!failOnError) return Collections.emptyMap();
            else throw new RuntimeException(e);
        }

        return Collections.emptyMap();
    }

    public static boolean isSelfRel(Relationship rel) {
        return Objects.equals(
                rel.getStartNode().getElementId(), rel.getEndNode().getElementId());
    }

    public static PointValue toPoint(Map<String, Object> pointMap, Map<String, Object> defaultPointMap) {
        double x;
        double y;
        Double z = null;

        final CoordinateReferenceSystem crs =
                CoordinateReferenceSystem.byName((String) getOrDefault(pointMap, defaultPointMap, "crs"));

        // It does not depend on the prefix of crs, I could also pass a point({x: 56.7, y: 12.78, crs: 'wgs-84'})
        final boolean isLatitudePresent = pointMap.containsKey("latitude")
                || (!pointMap.containsKey("x") && defaultPointMap.containsKey("latitude"));
        final boolean isCoord3D = crs.getName().endsWith("-3d");
        if (isLatitudePresent) {
            x = UtilExtended.toDouble(getOrDefault(pointMap, defaultPointMap, "longitude"));
            y = UtilExtended.toDouble(getOrDefault(pointMap, defaultPointMap, "latitude"));
            if (isCoord3D) {
                z = UtilExtended.toDouble(getOrDefault(pointMap, defaultPointMap, "height"));
            }
        } else {
            x = UtilExtended.toDouble(getOrDefault(pointMap, defaultPointMap, "x"));
            y = UtilExtended.toDouble(getOrDefault(pointMap, defaultPointMap, "y"));
            if (isCoord3D) {
                z = UtilExtended.toDouble(getOrDefault(pointMap, defaultPointMap, "z"));
            }
        }

        return z != null ? Values.pointValue(crs, x, y, z) : Values.pointValue(crs, x, y);
    }

    private static Object getOrDefault(Map<String, Object> firstMap, Map<String, Object> secondMap, String key) {
        return firstMap.getOrDefault(key, secondMap.get(key));
    }

    public static Object getStringOrCompressedData(StringWriter writer, ExportConfigExtended config) {
        try {
            final String compression = config.getCompressionAlgo();
            final String writerString = writer.toString();
            Object data = compression.equals(CompressionAlgoExtended.NONE.name())
                    ? writerString
                    : CompressionAlgoExtended.valueOf(compression).compress(writerString, config.getCharset());
            writer.getBuffer().setLength(0);
            return data;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T extends Entity> T withTransactionAndRebind(
            GraphDatabaseService db, Transaction transaction, Function<Transaction, T> action) {
        T result = retryInTx(NullLog.getInstance(), db, action, 0, 0, r -> {});
        return rebind(transaction, result);
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

    public static boolean constraintIsUnique(ConstraintType type) {
        return type == ConstraintType.NODE_KEY
                || type == ConstraintType.RELATIONSHIP_KEY
                || type == ConstraintType.UNIQUENESS
                || type == ConstraintType.RELATIONSHIP_UNIQUENESS;
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

    public static long getNodeId(InternalTransaction tx, String elementId) {
        return tx.elementIdMapper().nodeId(elementId);
    }

    public static long getRelationshipId(InternalTransaction tx, String elementId) {
        return tx.elementIdMapper().relationshipId(elementId);
    }

    public static String getNodeElementId(InternalTransaction tx, long id) {
        return tx.elementIdMapper().nodeElementId(id);
    }

    public static boolean isWindows() {
        return System.getProperty("os.name").toLowerCase().contains("win");
    }

    public static <T> boolean valueEquals(T one, T other) {
        if (one == null || other == null) {
            return false;
        }
        return ValueUtils.of(one).equals(ValueUtils.of(other));
    }

    public static boolean containsValueEquals(Collection<Object> collection, Object value) {
        return collection.stream().anyMatch(i -> UtilExtended.valueEquals(value, i));
    }

    public static <T> List<AnyValue> toAnyValues(List<T> list) {
        return list.stream().map(ValueUtils::of).collect(Collectors.toList());
    }

    public static <T> HashSet<Object> toAnyValuesSet(List<T> list) {
        var set = new HashSet<>(Math.max((int) (list.size() / .75f) + 1, 16));
        for (Object item : list) {
            // Use the ValueUtil.of version, as arrays and lists in Cypher may differ, but are considered the *same*.
            set.add(ValueUtils.of(item));
        }
        return set;
    }

    public static String toPrettyPrint(Object anyValue) {
        // Text values pretty print adds quotations, which is not generally
        // how APOC wants to print them.
        if (anyValue instanceof TextValue textValue) {
            return textValue.stringValue();
        } else if (anyValue instanceof Value value) {
            return value.prettyPrint();
        } else {
            return anyValue.toString();
        }
    }

    public static int indexOf(List<Object> list, Object value) {
        return IntStream.range(0, list.size())
                .filter((i) -> UtilExtended.valueEquals(list.get(i), value))
                .findFirst()
                .orElse(-1);
    }

    /*
     * Get all indexes from Neo4j
     * Currently filters out vector indexes
     * When vector indexes are supported, this can be changed to transaction.schema().getIndexes()
     */
    public static Iterable<IndexDefinition> getIndexes(Transaction transaction) {
        return StreamSupport.stream(transaction.schema().getIndexes().spliterator(), false)
                .filter(indexDefinition -> indexDefinition.getIndexType() != IndexType.VECTOR)
                .toList();
    }

    /*
     * Get all indexes from Neo4j for a given label
     * Currently filters out vector indexes
     * When vector indexes are supported, this can be changed to transaction.schema().getIndexes(label)
     */
    public static Iterable<IndexDefinition> getIndexes(Transaction transaction, Label label) {
        return StreamSupport.stream(transaction.schema().getIndexes(label).spliterator(), false)
                .filter(indexDefinition -> indexDefinition.getIndexType() != IndexType.VECTOR)
                .toList();
    }

    /*
     * Get all indexes from Neo4j for a given relationship type
     * Currently filters out vector indexes
     * When vector indexes are supported, this can be changed to transaction.schema().getIndexes(relType)
     */
    public static Iterable<IndexDefinition> getIndexes(Transaction transaction, RelationshipType relType) {
        return StreamSupport.stream(transaction.schema().getIndexes(relType).spliterator(), false)
                .filter(indexDefinition -> indexDefinition.getIndexType() != IndexType.VECTOR)
                .toList();
    }

    public static <T> T withBackOffRetries(Supplier<T> func, long initialTimeout, long upperTimeout, Log log) {
        T result = null;
        var startTime = System.currentTimeMillis();
        var timeout = initialTimeout;
        var lastTry = startTime - timeout;

        while (true) {
            var timeStamp = System.currentTimeMillis();
            if (timeStamp - lastTry >= timeout) {
                try {
                    result = func.get();
                    break;
                } catch (Exception e) {
                    if (timeout >= upperTimeout) {
                        Long totalTime = (System.currentTimeMillis() - startTime) / 1000;
                        if (log.isDebugEnabled()) {
                            log.debug(String.format(
                                    "Got %s: %s after %s seconds.", e.getClass(), e.getMessage(), totalTime));
                        }
                        throw e;
                    }
                }
                lastTry = timeStamp;
                timeout *= 2;
            }
        }
        return result;
    }
}
