package apoc.uuid;

import apoc.ExtendedSystemPropertyKeys;
import apoc.SystemPropertyKeys;
import apoc.util.Util;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.ApocConfig.*;
import static apoc.ExtendedApocConfig.APOC_UUID_ENABLED;
import static apoc.ExtendedApocConfig.APOC_UUID_ENABLED_DB;
import static apoc.ExtendedSystemLabels.ApocUuid;
import static apoc.ExtendedSystemLabels.ApocUuidMeta;
import static apoc.ExtendedSystemPropertyKeys.addToExistingNodes;
import static apoc.ExtendedSystemPropertyKeys.addToSetLabel;
import static apoc.ExtendedSystemPropertyKeys.propertyName;
import static apoc.util.SystemDbUtil.getSystemNodes;
import static apoc.util.SystemDbUtil.setLastUpdate;
import static apoc.util.SystemDbUtil.withSystemDb;
import static apoc.uuid.UuidHandler.NOT_ENABLED_ERROR;

public class UUIDHandlerNewProcedures {
    public static boolean isEnabled(String databaseName) {
        String apocUUIDEnabledDb = String.format(APOC_UUID_ENABLED_DB, databaseName);
        Configuration conf = apocConfig().getConfig();
        boolean enabled = conf.getBoolean(APOC_UUID_ENABLED, false);
        return conf.getBoolean(apocUUIDEnabledDb, enabled);
    }

    public static void checkEnabled(String databaseName) {
        if (!isEnabled(databaseName)) {
            String error = String.format(NOT_ENABLED_ERROR, databaseName);
            throw new RuntimeException(error);
        }
    }

    public static UuidInfo create(String databaseName, String label,  UuidConfig config) {
        final UuidInfo[] result = new UuidInfo[1];

        withSystemDb(sysTx -> {
            Node node = Util.mergeNode(sysTx, ApocUuid, null,
                    Pair.of(SystemPropertyKeys.database.name(), databaseName),
                    Pair.of(ExtendedSystemPropertyKeys.label.name(), label)
            );

            node.setProperty(propertyName.name(), config.getUuidProperty());
            node.setProperty(addToSetLabel.name(), config.isAddToSetLabels());
            node.setProperty(addToExistingNodes.name(), config.isAddToExistingNodes());

            // we'll the return current uuid info
            result[0] = new UuidInfo(node, true);

            setLastUpdate(sysTx, databaseName, ApocUuidMeta);
        });

        return result[0];
    }

    public static UuidInfo drop(String databaseName, String labelName) {
        return withSystemDb(tx -> {
            UuidInfo uuidInfo = getUuidNodes(tx, databaseName, Map.of(ExtendedSystemPropertyKeys.label.name(), labelName))
                    .stream()
                    .map(node -> {
                        UuidInfo info = new UuidInfo(node);
                        node.delete();
                        return info;
                    })
                    .findAny()
                    .orElse(null);

            setLastUpdate(tx, databaseName, ApocUuidMeta);

            return uuidInfo;
        });
    }

    public static List<UuidInfo> dropAll(String databaseName) {
        return withSystemDb(tx -> {
            List<UuidInfo> previous = getUuidNodes(tx, databaseName)
                    .stream()
                    .map(node -> {
                        // we'll return previous uuid info
                        UuidInfo info = new UuidInfo(node);
                        node.delete();
                        return info;
                    })
                    .collect(Collectors.toList());

            setLastUpdate(tx, databaseName, ApocUuidMeta);

            return previous;
        });
    }

    public static ResourceIterator<Node> getUuidNodes(Transaction tx, String databaseName) {
        return getUuidNodes(tx, databaseName, null);
    }

    public static ResourceIterator<Node> getUuidNodes(Transaction tx, String databaseName, Map<String, Object> props) {
        return getSystemNodes(tx, databaseName, ApocUuid, props);
    }

    public static void createConstraintUuid(Transaction tx, String label, String propertyName) {
        tx.execute(
                String.format("CREATE CONSTRAINT IF NOT EXISTS FOR (n:%s) REQUIRE (n.%s) IS UNIQUE",
                        Util.quote(label),
                        Util.quote(propertyName))
        );
    }
}
