package apoc.dv;

import apoc.ExtendedSystemLabels;
import apoc.ExtendedSystemPropertyKeys;
import apoc.SystemPropertyKeys;
import apoc.util.JsonUtil;
import apoc.util.Util;
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

// TODO - creare una nuova classe simile a questa, DataVirtualizationCatalogHandlerNewProcedures con i cambiamenti sottostanti
public class DataVirtualizationCatalogHandler {

    // todo - rimuovere 
    private final GraphDatabaseService db;
    // todo - rimuovere 
    private final GraphDatabaseService systemDb;
    // todo - rimuovere 
    private final Log log;

    // todo - rimuovere parametri
    public DataVirtualizationCatalogHandler(GraphDatabaseService db, GraphDatabaseService systemDb, Log log) {
        this.db = db;
        this.systemDb = systemDb;
        this.log = log;
    }

    // TODO - rimuovere metodo 
    private <T> T withSystemDb(Function<Transaction, T> action) {
        try (Transaction tx = systemDb.beginTx()) {
            T result = action.apply(tx);
            tx.commit();
            return result;
        }
    }

    // TODO - rinominare in install, e mettere `String databaseName` come primo parametro 
    public VirtualizedResource add(VirtualizedResource vr) {
        // TODO - withSystemDb dovrebbe essere quello preso da SystemDbUtil
        //      il resto dovrebbe rimanere uguale senza scoppiare
        return withSystemDb(tx -> {
            Node node = Util.mergeNode(tx, ExtendedSystemLabels.DataVirtualizationCatalog, null,
                    // TODO - cambiare db.databaseName() con databaseName
                    Pair.of(SystemPropertyKeys.database.name(), db.databaseName()),
                    Pair.of(SystemPropertyKeys.name.name(), vr.name));
            node.setProperty( ExtendedSystemPropertyKeys.data.name(), JsonUtil.writeValueAsString(vr));
            return vr;
        });
    }

    // TODO - rinominare in query, e mettere `String databaseName` come primo parametro
    public VirtualizedResource get(String name) {
        // TODO - come sopra
        return withSystemDb(tx -> {
            final List<Node> nodes = tx.findNodes(ExtendedSystemLabels.DataVirtualizationCatalog,
                            // TODO - cambiare db.databaseName() con databaseName
                    SystemPropertyKeys.database.name(), db.databaseName(),
                    SystemPropertyKeys.name.name(), name)
                .stream()
                .collect(Collectors.toList());
            if (nodes.size() > 1) {
                throw new RuntimeException("More than 1 result");
            }
            try {
                Node node = nodes.get(0);
                Map<String, Object> map = JsonUtil.OBJECT_MAPPER.readValue(node.getProperty(ExtendedSystemPropertyKeys.data.name()).toString(), Map.class);
                return VirtualizedResource.from(name, map);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }

    // TODO - rinominare in drop, e mettere `String databaseName` come primo parametro
    public Stream<VirtualizedResource> remove(String name) {
        withSystemDb(tx -> {
            tx.findNodes(ExtendedSystemLabels.DataVirtualizationCatalog,
                            // TODO - cambiare db.databaseName() con databaseName
                    SystemPropertyKeys.database.name(), db.databaseName(),
                    SystemPropertyKeys.name.name(), name)
                .stream()
                .forEach(Node::delete);
            return null;
        });
        return list();
    }

    // TODO - rinominare in show, e mettere `String databaseName` come primo parametro
    public Stream<VirtualizedResource> list() {
        return withSystemDb(tx ->
                tx.findNodes(ExtendedSystemLabels.DataVirtualizationCatalog,
                                // TODO - cambiare db.databaseName() con databaseName
                    SystemPropertyKeys.database.name(), db.databaseName())
                .stream()
                .map(node -> {
                    try {
                        Map<String, Object> map = JsonUtil.OBJECT_MAPPER.readValue(node.getProperty(ExtendedSystemPropertyKeys.data.name()).toString(), Map.class);
                        String name = node.getProperty(SystemPropertyKeys.name.name()).toString();
                        return VirtualizedResource.from(name, map);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList())
                .stream());
    }
}
