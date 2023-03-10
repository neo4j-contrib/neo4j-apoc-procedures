package apoc.uuid;

import apoc.ApocConfig;
import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.Util;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.ApocConfig.*;
import static apoc.SystemPropertyKeys.*;
import static apoc.SystemLabels.*;
import static apoc.util.SystemDbUtil.getSystemNodes;
import static apoc.util.SystemDbUtil.setLastUpdate;
import static apoc.util.SystemDbUtil.withSystemDb;
import static apoc.uuid.UuidHandler.NOT_ENABLED_ERROR;

public class UUIDHandlerNewProcedures {
    public static boolean isEnabled(String databaseName) {
        String apocUUIDEnabledDb = String.format(ApocConfig.APOC_UUID_ENABLED_DB, databaseName);
        return apocConfig().getConfig().getBoolean(apocUUIDEnabledDb, apocConfig().getBoolean(APOC_UUID_ENABLED));
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
            Node node = Util.mergeNode(sysTx, SystemLabels.ApocUuid, null,
                    Pair.of(database.name(), databaseName),
                    Pair.of(SystemPropertyKeys.label.name(), label)
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
        final UuidInfo[] previous = new UuidInfo[1];

        withSystemDb(tx -> {
            getUuidNodes(tx, databaseName, Map.of(SystemPropertyKeys.label.name(), labelName))
                    .forEachRemaining(node -> {
                        previous[0] = new UuidInfo(node);
                        node.delete();
                    });

            setLastUpdate(tx, databaseName, ApocUuidMeta);
        });

        return previous[0];
    }

    public static List<UuidInfo> dropAll(String databaseName) {
        final List<UuidInfo> previous = new ArrayList<>();

        withSystemDb(tx -> {
            getUuidNodes(tx, databaseName)
                    .forEachRemaining(node -> {
                        // we'll return previous uuid info
                        previous.add( new UuidInfo(node) );
                        node.delete();
                    });

            setLastUpdate(tx, databaseName, ApocUuidMeta);
        });

        return previous;
    }

    public static ResourceIterator<Node> getUuidNodes(Transaction tx, String databaseName) {
        return getUuidNodes(tx, databaseName, null);
    }

    public static ResourceIterator<Node> getUuidNodes(Transaction tx, String databaseName, Map<String, Object> props) {
        return getSystemNodes(tx, databaseName, SystemLabels.ApocUuid, props);
    }

    public static void checkConstraintUuid(Transaction tx, String label, String propertyName) {
        Schema schema = tx.schema();
        Stream<ConstraintDefinition> constraintDefinitionStream = StreamSupport.stream(schema.getConstraints(Label.label(label)).spliterator(), false);
        boolean exists = constraintDefinitionStream.anyMatch(constraint -> {
            Stream<String> streamPropertyKeys = StreamSupport.stream(constraint.getPropertyKeys().spliterator(), false);
            return streamPropertyKeys.anyMatch(property -> property.equals(propertyName));
        });
        if (!exists) {
            String error = String.format("`CREATE CONSTRAINT ON (%s:%s) ASSERT %s.%s IS UNIQUE`",
                    label.toLowerCase(), label, label.toLowerCase(), propertyName);
            throw new RuntimeException("No constraint found for label: " + label + ", please add the constraint with the following : " + error);
        }
    }
}
