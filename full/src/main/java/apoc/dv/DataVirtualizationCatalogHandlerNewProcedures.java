package apoc.dv;

import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.JsonUtil;
import apoc.util.Util;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.Node;

import java.util.Map;
import java.util.stream.Stream;

import static apoc.util.SystemDbUtil.withSystemDb;

public class DataVirtualizationCatalogHandlerNewProcedures {

    public DataVirtualizationCatalogHandlerNewProcedures() {}

    public VirtualizedResource install(String databaseName, VirtualizedResource vr) {
        return withSystemDb(tx -> {
            Node node = Util.mergeNode(tx, ExtendedSystemLabels.DataVirtualizationCatalog, null,
                    Pair.of(SystemPropertyKeys.database.name(), databaseName),
                    Pair.of(SystemPropertyKeys.name.name(), vr.name));
            node.setProperty( ExtendedSystemPropertyKeys.data.name(), JsonUtil.writeValueAsString(vr));
            return vr;
        });
    }

    public Stream<VirtualizedResource> drop(String databaseName, String name) {
        withSystemDb(tx -> {
            tx.findNodes(ExtendedSystemLabels.DataVirtualizationCatalog,
                            SystemPropertyKeys.database.name(), databaseName,
                            SystemPropertyKeys.name.name(), name)
                    .stream()
                    .forEach(Node::delete);
            return null;
        });
        return show(databaseName);
    }

    public Stream<VirtualizedResource> show(String databaseName) {
        return withSystemDb(tx -> {
            return tx.findNodes(ExtendedSystemLabels.DataVirtualizationCatalog,
                            SystemPropertyKeys.database.name(), databaseName)
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
                    .toList()
                    .stream();
        });
    }
}
