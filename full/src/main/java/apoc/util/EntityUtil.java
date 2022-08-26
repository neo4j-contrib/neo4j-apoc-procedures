package apoc.util;

import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;

import java.util.Map;
import java.util.stream.Collectors;

public class EntityUtil {
    
    public static <T> T anyRebind(Transaction tx, T any) {
        if (any instanceof Map) {
            return (T) ((Map<String, Object>) any).entrySet().stream()
                    .collect(Collectors.toMap(e -> e.getKey(), e -> anyRebind(tx, e.getValue())));
        }
        if (any instanceof Path) {
            final Path path = (Path) any;
            PathImpl.Builder builder = new PathImpl.Builder(Util.rebind(tx, path.startNode()));
            for (Relationship rel: path.relationships()) {
                builder = builder.push(Util.rebind(tx, rel));
            }
            return (T) builder.build();
        }
        if (any instanceof Iterable) {
            return (T) Iterables.stream((Iterable) any)
                    .map(i -> anyRebind(tx, i)).collect(Collectors.toList());
        }
        if (any instanceof Entity) {
            return (T) Util.rebind(tx, (Entity) any);
        }
        return any;
    }
}
