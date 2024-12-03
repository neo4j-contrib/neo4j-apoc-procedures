package apoc.dv;

import apoc.ExtendedSystemLabels;
import apoc.ExtendedSystemPropertyKeys;
import apoc.util.JsonUtilExtended;
import apoc.util.UtilExtended;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataVirtualizationCatalogHandler {

    private final GraphDatabaseService db;
    private final GraphDatabaseService systemDb;
    private final Log log;

    public DataVirtualizationCatalogHandler(GraphDatabaseService db, GraphDatabaseService systemDb, Log log) {
        this.db = db;
        this.systemDb = systemDb;
        this.log = log;
    }


    private <T> T withSystemDb(Function<Transaction, T> action) {
        try (Transaction tx = systemDb.beginTx()) {
            T result = action.apply(tx);
            tx.commit();
            return result;
        }
    }

    public VirtualizedResource add(VirtualizedResource vr) {
        return withSystemDb(tx -> {
            Node node = UtilExtended.mergeNode(tx, ExtendedSystemLabels.DataVirtualizationCatalog, null,
                    Pair.of(ExtendedSystemPropertyKeys.database.name(), db.databaseName()),
                    Pair.of(ExtendedSystemPropertyKeys.name.name(), vr.name));
            node.setProperty( ExtendedSystemPropertyKeys.data.name(), JsonUtilExtended.writeValueAsString(vr));
            return vr;
        });
    }

    public VirtualizedResource get(String name) {
        return withSystemDb(tx -> {
            final List<Node> nodes = tx.findNodes(ExtendedSystemLabels.DataVirtualizationCatalog,
                            ExtendedSystemPropertyKeys.database.name(), db.databaseName(),
                            ExtendedSystemPropertyKeys.name.name(), name)
                .stream()
                .collect(Collectors.toList());
            if (nodes.size() > 1) {
                throw new RuntimeException("More than 1 result");
            }
            try {
                Node node = nodes.get(0);
                Map<String, Object> map = JsonUtilExtended.OBJECT_MAPPER.readValue(node.getProperty(ExtendedSystemPropertyKeys.data.name()).toString(), Map.class);
                return VirtualizedResource.from(name, map);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public Stream<VirtualizedResource> remove(String name) {
        withSystemDb(tx -> {
            tx.findNodes(ExtendedSystemLabels.DataVirtualizationCatalog,
                            ExtendedSystemPropertyKeys.database.name(), db.databaseName(),
                            ExtendedSystemPropertyKeys.name.name(), name)
                .stream()
                .forEach(Node::delete);
            return null;
        });
        return list();
    }

    public Stream<VirtualizedResource> list() {
        return withSystemDb(tx ->
                tx.findNodes(ExtendedSystemLabels.DataVirtualizationCatalog,
                                ExtendedSystemPropertyKeys.database.name(), db.databaseName())
                .stream()
                .map(node -> {
                    try {
                        Map<String, Object> map = JsonUtilExtended.OBJECT_MAPPER.readValue(node.getProperty(ExtendedSystemPropertyKeys.data.name()).toString(), Map.class);
                        String name = node.getProperty(ExtendedSystemPropertyKeys.name.name()).toString();
                        return VirtualizedResource.from(name, map);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList())
                .stream());
    }
}
