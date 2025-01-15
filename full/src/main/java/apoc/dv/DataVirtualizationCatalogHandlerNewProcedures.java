package apoc.dv;

import static apoc.util.SystemDbUtil.withSystemDb;

import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.JsonUtil;
import apoc.util.Util;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.helpers.collection.Pair;

public class DataVirtualizationCatalogHandlerNewProcedures {

    public DataVirtualizationCatalogHandlerNewProcedures() {}

    public VirtualizedResource install(String databaseName, VirtualizedResource vr) {
        return withSystemDb(tx -> {
            Node node = Util.mergeNode(
                    tx,
                    SystemLabels.DataVirtualizationCatalog,
                    null,
                    Pair.of(SystemPropertyKeys.database.name(), databaseName),
                    Pair.of(SystemPropertyKeys.name.name(), vr.name));
            node.setProperty(SystemPropertyKeys.data.name(), JsonUtil.writeValueAsString(vr));
            return vr;
        });
    }

    public Stream<VirtualizedResource> drop(String databaseName, String name) {
        withSystemDb(tx -> {
            tx
                    .findNodes(
                            SystemLabels.DataVirtualizationCatalog,
                            SystemPropertyKeys.database.name(),
                            databaseName,
                            SystemPropertyKeys.name.name(),
                            name)
                    .stream()
                    .forEach(Node::delete);
            return null;
        });
        return show(databaseName);
    }

    public Stream<VirtualizedResource> show(String databaseName) {
        return withSystemDb(tx -> {
            return tx
                    .findNodes(
                            SystemLabels.DataVirtualizationCatalog,
                            SystemPropertyKeys.database.name(),
                            databaseName)
                    .stream()
                    .map(node -> {
                        try {
                            Map<String, Object> map = JsonUtil.OBJECT_MAPPER.readValue(
                                    node.getProperty(SystemPropertyKeys.data.name())
                                            .toString(),
                                    Map.class);
                            String name = node.getProperty(SystemPropertyKeys.name.name())
                                    .toString();
                            return VirtualizedResource.from(name, map);
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(Collectors.toList())
                    .stream();
        });
    }
}
