package apoc.util;

import apoc.ApocConfiguration;
import apoc.Pools;
import apoc.path.RelationshipTypeAndDirections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Name;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.*;
import java.util.zip.DeflaterInputStream;
import java.util.zip.GZIPInputStream;

import static java.lang.String.format;

/**
 * @author mh
 * @since 24.04.16
 */
public class Util {
    public static final Label[] NO_LABELS = new Label[0];
    public static final String NODE_COUNT = "MATCH (n) RETURN count(*) as result";
    public static final String REL_COUNT = "MATCH ()-->() RETURN count(*) as result";


    public static String labelString(Node n) {
        return StreamSupport.stream(n.getLabels().spliterator(),false).map(Label::name).sorted().collect(Collectors.joining(":"));
    }
    public static Label[] labels(Object labelNames) {
        if (labelNames==null) return NO_LABELS;
        if (labelNames instanceof List) {
            List names = (List) labelNames;
            Label[] labels = new Label[names.size()];
            int i = 0;
            for (Object l : names) {
                if (l==null) continue;
                labels[i++] = Label.label(l.toString());
            }
            if (i <= labels.length) return Arrays.copyOf(labels,i);
            return labels;
        }
        return new Label[]{Label.label(labelNames.toString())};
    }

    public static RelationshipType type(Object type) {
        if (type == null) throw new RuntimeException("No relationship-type provided");
        return RelationshipType.withName(type.toString());
    }

    @SuppressWarnings("unchecked")
    public static LongStream ids(Object ids) {
        if (ids == null) return LongStream.empty();
        if (ids instanceof Number) return LongStream.of(((Number)ids).longValue());
        if (ids instanceof Collection) {
            Collection<Object> coll = (Collection<Object>) ids;
            return coll.stream().mapToLong( (o) -> ((Number)o).longValue());
        }
        if (ids instanceof Iterable) {
            Spliterator<Object> spliterator = ((Iterable) ids).spliterator();
            return StreamSupport.stream(spliterator,false).mapToLong( (o) -> ((Number)o).longValue());
        }
        throw new RuntimeException("Can't convert "+ids.getClass()+" to a stream of long ids");
    }

    public static Stream<Relationship> relsStream(GraphDatabaseService db, Object ids) {
        return ids(ids).mapToObj(db::getRelationshipById);
    }

    public static Stream<Node> nodeStream(GraphDatabaseService db, @Name("nodes") Object ids) {
        return ids(ids).mapToObj(db::getNodeById);
    }

    public static double doubleValue(PropertyContainer pc, String prop, Number defaultValue) {
        return toDouble(pc.getProperty(prop, defaultValue));

    }

    public static double doubleValue(PropertyContainer pc, String prop) {
        return doubleValue(pc, prop, 0);
    }

    public static Direction parseDirection(String direction) {
        if (null == direction) {
            return Direction.BOTH;
        }
        try {
            return Direction.valueOf(direction.toUpperCase());
        } catch (Exception e) {
            throw new RuntimeException(format("Cannot convert value '%s' to Direction. Legal values are '%s'",
                    direction, Arrays.toString(Direction.values())));
        }
    }

    public static RelationshipType[] toRelTypes(List<String> relTypeStrings) {
        RelationshipType[] relTypes = new RelationshipType[relTypeStrings.size()];
        for (int i = 0; i < relTypes.length; i++) {
            relTypes[i] = RelationshipType.withName(relTypeStrings.get(i));
        }
        return relTypes;
    }

    public static RelationshipType[] allRelationshipTypes(GraphDatabaseService db) {
        return Iterables.asArray(RelationshipType.class, db.getAllRelationshipTypes());
    }

    public static RelationshipType[] typesAndDirectionsToTypesArray(String typesAndDirections) {
        List<RelationshipType> relationshipTypes = new ArrayList<>();
        for (Pair<RelationshipType, Direction> pair : RelationshipTypeAndDirections.parse(typesAndDirections)) {
            if (null != pair.first()) {
                relationshipTypes.add(pair.first());
            }
        }
        return relationshipTypes.toArray(new RelationshipType[relationshipTypes.size()]);
    }

    public static <T> T inTx(GraphDatabaseAPI db, Callable<T> callable) {
        try {
            return Pools.DEFAULT.submit(() -> {
                try (Transaction tx = db.beginTx()) {
                    T result = callable.call();
                    tx.success();
                    return result;
                }
            }).get();
        } catch (Exception e) {
            throw new RuntimeException("Error executing in separate transaction", e);
        }
    }
    public static <T> T inThread(Callable<T> callable) {
        try {
            return Pools.DEFAULT.submit(callable::call).get();
        } catch (Exception e) {
            throw new RuntimeException("Error executing in separate thread", e);
        }
    }

    public static Double toDouble(Object value) {
        if (value == null) return null;
        if (value instanceof Number) return ((Number) value).doubleValue();
        return Double.parseDouble(value.toString());
    }

    public static Map<String, Object> subMap(Map<String, ?> params, String prefix) {
        Map<String, Object> config = new HashMap<>(10);
        int len = prefix.length() + (prefix.endsWith(".") ? 0 : 1);
        for (Map.Entry<String, ?> entry : params.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(prefix)) {
                config.put(key.substring(len), entry.getValue());
            }
        }
        return config;
    }

    public static long toLong(Object value) {
        if (value instanceof Number) return ((Number)value).longValue();
        return Long.parseLong(value.toString());
    }

    public static URLConnection openUrlConnection(String url, Map<String, Object> headers) throws IOException {
        URL src = new URL(url);
        URLConnection con = src.openConnection();
        con.setRequestProperty("User-Agent", "APOC Procedures for Neo4j");
        if (headers != null) {
            Object method = headers.get("method");
            if (method != null && con instanceof HttpURLConnection) {
                HttpURLConnection http = (HttpURLConnection) con;
                http.setRequestMethod(method.toString());
                http.setChunkedStreamingMode(1024*1024);
                http.setInstanceFollowRedirects(true);
            }
            headers.forEach((k,v) -> con.setRequestProperty(k, v == null ? "" : v.toString()));
        }
        con.setDoInput(true);
        con.setConnectTimeout((int)toLong(ApocConfiguration.get("http.timeout.connect",10_000)));
        con.setReadTimeout((int)toLong(ApocConfiguration.get("http.timeout.read",60_000)));
        return con;
    }

    public static InputStream openInputStream(String url, Map<String, Object> headers, String payload) throws IOException {
        URLConnection con = openUrlConnection(url, headers);
        if (payload != null) {
            con.setDoOutput(true);
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(con.getOutputStream(),"UTF-8"));
            writer.write(payload);
            writer.close();
        }

        InputStream stream = con.getInputStream();

        String encoding = con.getContentEncoding();
        if ("gzip".equals(encoding) || url.endsWith(".gz")) {
            stream = new GZIPInputStream(stream);
        }
        if ("deflate".equals(encoding)) {
            stream = new DeflaterInputStream(stream);
        }
        return stream;
    }

    public static boolean toBoolean(Object value) {
        if ((value == null || value instanceof Number && (((Number) value).longValue()) == 0L || value instanceof String && (value.equals("") || ((String) value).equalsIgnoreCase("false") || ((String) value).equalsIgnoreCase("no")|| ((String) value).equalsIgnoreCase("0"))|| value instanceof Boolean && value.equals(false))) {
            return false;
        }
        return true;
    }

    public static String encodeUrlComponent(String value) {
        try {
            return URLEncoder.encode(value,"UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unsupported character set utf-8");
        }
    }

    public static String toJson(Object value) {
        try {
            return JsonUtil.OBJECT_MAPPER.writeValueAsString(value);
        } catch (IOException e) {
            throw new RuntimeException("Can't convert "+value+" to JSON");
        }
    }

    public static Stream<List<Object>> partitionSubList(List<Object> data, int partitions) {
        List<Object> list = new ArrayList<>(data);
        int total = list.size();
        int batchSize = Math.max((int)Math.ceil((double)total / partitions),1);
        return IntStream.rangeClosed(0, partitions).parallel()
                .mapToObj((part) -> list.subList(Math.min(part * batchSize,total), Math.min((part + 1) * batchSize, total)))
                .filter(partition -> !partition.isEmpty());
    }

    public static Long runNumericQuery(GraphDatabaseService db, String query, Map<String, Object> params) {
        if (params == null) params = Collections.emptyMap();
        try (ResourceIterator<Long> it = db.execute(query,params).<Long>columnAs("result")) {
            return it.next();
        }
    }

    public static long nodeCount(GraphDatabaseService db) {
        return runNumericQuery(db,NODE_COUNT,null);
    }
    public static long relCount(GraphDatabaseService db) {
        return runNumericQuery(db,REL_COUNT,null);
    }

    public static LongStream toLongStream(PrimitiveLongIterator it) {
        PrimitiveIterator.OfLong iterator = new PrimitiveIterator.OfLong() {

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public long nextLong() {
                return it.next();
            }
        };
        return StreamSupport.longStream(Spliterators.spliteratorUnknownSize(
                iterator,
                Spliterator.ORDERED | Spliterator.IMMUTABLE | Spliterator.NONNULL), false);
    }

    public static String readResourceFile(String name) {
		InputStream is = Util.class.getClassLoader().getResourceAsStream(name);
		return new Scanner(is).useDelimiter("\\Z").next();
	}
}
