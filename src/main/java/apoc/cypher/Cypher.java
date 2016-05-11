package apoc.cypher;

import apoc.Pools;
import apoc.result.MapResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.lang.String.join;

/**
 * @author mh
 * @since 08.05.16
 */
public class Cypher {

    public static final String COMPILED_PREFIX = ""; //  "CYPHER runtime=compiled ";
    public static final ExecutorService POOL = Pools.DEFAULT;
    public static final int PARTITIONS = 100 * Runtime.getRuntime().availableProcessors();
    @Context public GraphDatabaseService db;
    @Context public GraphDatabaseAPI api;

    @Procedure
    public Stream<MapResult> run(@Name("cypher") String statement, @Name("params") Map<String,Object> params) {
        if (params==null) params = Collections.emptyMap();
        return db.execute(compiled(statement, params.keySet()),params).stream().map(MapResult::new);
    }

    public String compiled(@Name("cypher") String fragment, Collection<String> keys) {
        String declaration = " WITH "+ join(", ", keys.stream().map(s -> format(" {`%s`} as `%s` ", s, s)).collect(Collectors.toList()));

        return declaration + fragment;
//        return fragment.substring(0,6).equalsIgnoreCase("cypher") ? fragment : COMPILED_PREFIX + fragment;
    }

    @Procedure
    public Stream<MapResult> parallel(@Name("fragment") String fragment, @Name("params") Map<String,Object> params, @Name("parallelizeOn") String key) {
        if (params==null) return run(fragment,params);
        if (key == null || !params.containsKey(key)) throw new RuntimeException("Can't parallelize on key "+key+" available keys "+params.keySet());
        Object value = params.get(key);
        if (!(value instanceof Collection)) throw new RuntimeException("Can't parallelize a non collection "+key+" : "+value);

        final String statement = compiled(fragment,params.keySet());
        Collection<Object> coll = (Collection<Object>) value;
        return coll.parallelStream().flatMap( (v) -> {
            Map<String,Object> parallelParams = new HashMap<>(params);
            parallelParams.replace(key,v);
            return api.execute(statement,parallelParams).stream().map(MapResult::new);
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
    public Stream<MapResult> mapParallel(@Name("fragment") String fragment, @Name("params") Map<String,Object> params, @Name("list") List<Object> data) {
        final String statement = parallelStatement(fragment, params, "_");
        db.execute("EXPLAIN "+statement).close();
        return partitionSubList(data, PARTITIONS)
                .flatMap((partition) -> Iterators.asList(api.execute(statement, parallelParams(params, "_", partition))).stream())
                .map(MapResult::new);
    }

    public Stream<List<Object>> partitionSubList(@Name("list") List<Object> data, int partitions) {
        List<Object> list = new ArrayList<>(data);
        int total = list.size();
        int batchSize = Math.max(total / partitions,1);
        return IntStream.rangeClosed(0, partitions).parallel()
                .mapToObj((part) -> list.subList(part * batchSize, Math.min((part + 1) * batchSize, total)))
                .filter(partition -> !partition.isEmpty());
    }

    // todo proper Collector
    public Stream<List<Object>> partitionColl(@Name("list") Collection<Object> list, int partitions) {
        int total = list.size();
        int batchSize = Math.max(total / partitions,1);
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
    public Stream<MapResult> parallel2(@Name("fragment") String fragment, @Name("params") Map<String,Object> params, @Name("parallelizeOn") String key) {
        if (params==null) return run(fragment,params);
        if (key == null || !params.containsKey(key)) throw new RuntimeException("Can't parallelize on key "+key+" available keys "+params.keySet());
        Object value = params.get(key);
        if (!(value instanceof Collection)) throw new RuntimeException("Can't parallelize a non collection "+key+" : "+value);

        final String statement = parallelStatement(fragment, params, key);
        db.execute("EXPLAIN "+statement).close();
        Collection<Object> coll = (Collection<Object>) value;
        int total = coll.size();
        int batchSize = Math.max(total / PARTITIONS,1);

        List<Future<List<Map<String,Object>>>> futures = new ArrayList<>(PARTITIONS);
        List<Object> partition = new ArrayList<>(batchSize);
        for (Object o : coll) {
            partition.add(o);
            if (partition.size() == batchSize) {
                futures.add(submit(api,statement,params,key,partition));
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
                throw new RuntimeException("Error executing in parallell "+statement,e);
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
            }
            else {
                sb.append(", ");
            }
            sb.append(format(" {`%s`} as `%s` ", s, s));
        }
        sb.append("UNWIND {`").append(key).append("`} as `").append(key).append("` ");
        return sb.toString() + fragment;
    }

    private Future<List<Map<String, Object>>> submit(GraphDatabaseAPI api, String statement, Map<String, Object> params, String key, List<Object> partition) {
        return POOL.submit(() -> Iterators.asList(api.execute(statement, parallelParams(params,key,partition))));
    }

    private static Collection asCollection(Object value) {
        if (value instanceof Collection) return (Collection)value;
        if (value instanceof Iterable) return Iterables.asCollection((Iterable)value);
        if (value instanceof Iterator) return Iterators.asCollection((Iterator)value);
        return Collections.singleton(value);
    }

    @Procedure("cypher.do") @PerformsWrites
    public Stream<MapResult> doit(@Name("cypher") String statement, @Name("params") Map<String,Object> params) {
        if (params==null) params = Collections.emptyMap();
        return db.execute(compiled(statement,params.keySet()),params).stream().map(MapResult::new);
    }
}
