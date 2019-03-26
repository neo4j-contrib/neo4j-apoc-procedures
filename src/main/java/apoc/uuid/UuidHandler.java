package apoc.uuid;

import apoc.util.JsonUtil;
import apoc.util.Util;
import com.fasterxml.jackson.core.type.TypeReference;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.core.GraphProperties;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class UuidHandler implements TransactionEventHandler {

    private static final String APOC_UUID = "apoc.uuid";
    static ConcurrentHashMap<String, UuidConfig> uuid = new ConcurrentHashMap();
    private static GraphProperties properties;
    private final Log log;


    private static final TypeReference<Map<String, UuidConfig>> typeRef = new TypeReference<Map<String, UuidConfig>>() {};

    private static final String NOT_ENABLED_ERROR = "UUID have not been enabled." +
            " Set 'apoc.uuid.enabled=true' in your neo4j.conf file located in the $NEO4J_HOME/conf/ directory.";

    UuidHandler(GraphDatabaseAPI api, Log log) {
        properties = api.getDependencyResolver().resolveDependency(EmbeddedProxySPI.class).newGraphPropertiesProxy();
        this.log = log;
    }

    private static void checkEnabled() {
        if (properties == null) {
            throw new RuntimeException(NOT_ENABLED_ERROR);
        }
    }

    public static UuidConfig add(String label, UuidConfig config) {
        checkEnabled();
        checkConstraintUuid(label, config);
        UuidConfig oldConfig = updateUuid(label, config);
        return oldConfig;
    }

    private synchronized static UuidConfig updateUuid(String label, UuidConfig value) {
        checkEnabled();
        try (Transaction tx = properties.getGraphDatabase().beginTx()) {
            uuid.clear();
            String uuidProperty = (String) properties.getProperty(APOC_UUID, "{}");
            uuid.putAll(JsonUtil.OBJECT_MAPPER.readValue(uuidProperty, typeRef));
            UuidConfig previous = null;
            if (label != null) {
                previous = (value == null) ? uuid.remove(label) : uuid.put(label, value);
                if (value != null || previous != null) {
                    properties.setProperty(APOC_UUID, Util.toJson(uuid));
                }
            }
            tx.success();
            return previous;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object beforeCommit(TransactionData txData) {
        GraphDatabaseService db = properties.getGraphDatabase();
        Map<String, String> exceptions = new LinkedHashMap<>();
        Iterable<Node> createdNodes = txData.createdNodes();
        Iterable<PropertyEntry<Node>> assignedNodeProperties = txData.assignedNodeProperties();
        Iterable<PropertyEntry<Node>> removedNodeProperties = txData.removedNodeProperties();

        uuid.forEach((label, config) -> {
            try (Transaction tx = db.beginTx()) {
                if (createdNodes.iterator().hasNext()) {
                    createdNodes.forEach(node -> {
                        if (node.hasLabel(Label.label(label)) && !node.hasProperty(config.getUuidProperty())) {
                            String uuid = UUID.randomUUID().toString();
                            node.setProperty(config.getUuidProperty(), uuid);
                        }
                    });
                }
                checkAndRestoreUuidProperty(assignedNodeProperties, label, config.getUuidProperty(),
                        (nodePropertyEntry) -> nodePropertyEntry.value() == null || nodePropertyEntry.value().equals(""));
                checkAndRestoreUuidProperty(removedNodeProperties, label, config.getUuidProperty());
                tx.success();
            } catch (Exception e) {
                log.warn("Error executing uuid " + label + " in phase before", e);
                exceptions.put(label, e.getMessage());
            }
        });
        return null;
    }

    private void checkAndRestoreUuidProperty(Iterable<PropertyEntry<Node>> nodeProperties, String label, String uuidProperty) {
        checkAndRestoreUuidProperty(nodeProperties, label, uuidProperty, null);
    }

    private void checkAndRestoreUuidProperty(Iterable<PropertyEntry<Node>> nodeProperties, String label, String uuidProperty, Predicate<PropertyEntry<Node>> predicate) {
        if (nodeProperties.iterator().hasNext()) {
            nodeProperties.forEach(nodePropertyEntry -> {
                if (predicate == null) {
                    if (nodePropertyEntry.entity().hasLabel(Label.label(label)) && nodePropertyEntry.key().equals(uuidProperty)) {
                        nodePropertyEntry.entity().setProperty(uuidProperty, nodePropertyEntry.previouslyCommitedValue());
                    }
                } else {
                    if (nodePropertyEntry.entity().hasLabel(Label.label(label)) && nodePropertyEntry.key().equals(uuidProperty) && predicate.test(nodePropertyEntry)) {
                        nodePropertyEntry.entity().setProperty(uuidProperty, nodePropertyEntry.previouslyCommitedValue());
                    }
                }
            });
        }
    }

    @Override
    public void afterCommit(TransactionData data, Object state) {
    }

    @Override
    public void afterRollback(TransactionData data, Object state) {
    }

    public static Map<String, UuidConfig> list() {
        checkEnabled();
        updateUuid(null, null);
        return uuid;
    }

    public static void checkConstraintUuid(String label, UuidConfig config) {
        GraphDatabaseService db = properties.getGraphDatabase();
        try (Transaction tx = db.beginTx()) {
            Schema schema = db.schema();
            Stream<ConstraintDefinition> constraintDefinitionStream = StreamSupport.stream(schema.getConstraints(Label.label(label)).spliterator(), false);
            boolean exists = constraintDefinitionStream.anyMatch(constraint -> {
                Stream<String> streamPropertyKeys = StreamSupport.stream(constraint.getPropertyKeys().spliterator(), false);
                return streamPropertyKeys.anyMatch(property -> property.equals(config.getUuidProperty()));
            });
            if (!exists) {
                String error = String.format("`CREATE CONSTRAINT ON (%s:%s) ASSERT %s.%s IS UNIQUE`",
                        label.toLowerCase(), label, label.toLowerCase(), config.getUuidProperty());
                throw new RuntimeException("No constraint found for label: " + label + ", please add the constraint with the following : " + error);
            }
            tx.success();
        }
    }

    public synchronized static UuidConfig remove(String label) {
        return updateUuid(label, null);
    }

    public synchronized static Map<String, Object> removeAll() {
        try (Transaction tx = properties.getGraphDatabase().beginTx()) {
            uuid.clear();
            String previous = (String) properties.removeProperty(APOC_UUID);
            tx.success();
            return previous == null ? null : Util.fromJson(previous, Map.class);
        } catch (Exception e) {
            return null;
        }
    }
}