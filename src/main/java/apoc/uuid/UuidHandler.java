package apoc.uuid;

import apoc.util.Util;
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

import static apoc.util.Util.map;
import static apoc.util.Util.toBoolean;

public class UuidHandler implements TransactionEventHandler {

    private static final String APOC_UUID = "apoc.uuid";
    private static final String UUID_PROPERTY = "uuid";
    static ConcurrentHashMap<String, Map<String, Object>> uuid = new ConcurrentHashMap(map("", map()));
    private static GraphProperties properties;
    private final Log log;

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

    public static Map<String, Object> add(String label, Map<String, Object> params) {
        checkEnabled();
        checkConstraintUuid(label);
        Map<String, Object> stringObjectMap = updateUuid(label, map("params", params));
        if (toBoolean(params.getOrDefault("addToAlreadyPresent", true))) {
            GraphDatabaseService db = properties.getGraphDatabase();
            try (Transaction tx = db.beginTx()) {
                db.findNodes(Label.label(label)).stream().filter(node -> !node.hasProperty(UUID_PROPERTY)).forEach(node -> {
                    String uuid = UUID.randomUUID().toString();
                    node.setProperty(UUID_PROPERTY, uuid);
                });
                tx.success();
            }
        }
        return stringObjectMap;
    }

    private synchronized static Map<String, Object> updateUuid(String label, Map<String, Object> value) {
        checkEnabled();
        try (Transaction tx = properties.getGraphDatabase().beginTx()) {
            uuid.clear();
            String uuidProperty = (String) properties.getProperty(APOC_UUID, "{}");
            uuid.putAll(Util.fromJson(uuidProperty, Map.class));
            Map<String, Object> previous = null;
            if (label != null) {
                previous = (value == null) ? uuid.remove(label) : uuid.put(label, value);
                if (value != null || previous != null) {
                    properties.setProperty(APOC_UUID, Util.toJson(uuid));
                }
            }
            tx.success();
            return previous;
        }
    }

    @Override
    public Object beforeCommit(TransactionData txData) throws Exception {
        if (uuid.containsKey("")) updateUuid(null,null);
        GraphDatabaseService db = properties.getGraphDatabase();
        Map<String, String> exceptions = new LinkedHashMap<>();
        Iterable<Node> createdNodes = txData.createdNodes();
        Iterable<PropertyEntry<Node>> assignedNodeProperties = txData.assignedNodeProperties();
        Iterable<PropertyEntry<Node>> removedNodeProperties = txData.removedNodeProperties();

        uuid.forEach((label, config) -> {
            try (Transaction tx = db.beginTx()) {
                if (createdNodes.iterator().hasNext()) {
                    createdNodes.forEach(node -> {
                        boolean containsLabel = containsLabel(label, node.getLabels());
                        if (containsLabel && !node.hasProperty(UUID_PROPERTY)) {
                            String uuid = UUID.randomUUID().toString();
                            node.setProperty(UUID_PROPERTY, uuid);
                        }
                    });
                }
                checkAndRestoreUuidProperty(assignedNodeProperties, label, (nodePropertyEntry) -> nodePropertyEntry.value().equals("") || nodePropertyEntry.value() == null);
                checkAndRestoreUuidProperty(removedNodeProperties, label, null);
                tx.success();
            } catch (Exception e) {
                log.warn("Error executing uuid " + label + " in phase before", e);
                exceptions.put(label, e.getMessage());
            }
        });
        return null;
    }

    private void checkAndRestoreUuidProperty(Iterable<PropertyEntry<Node>> nodeProperties, String label, Predicate<PropertyEntry<Node>> predicate) {
        if (nodeProperties.iterator().hasNext()) {
            nodeProperties.forEach(nodePropertyEntry -> {
                boolean containsLabel = containsLabel(label, nodePropertyEntry.entity().getLabels());
                if (predicate == null) {
                    if (containsLabel && nodePropertyEntry.key().equals(UUID_PROPERTY)) {
                        nodePropertyEntry.entity().setProperty(UUID_PROPERTY, nodePropertyEntry.previouslyCommitedValue());
                    }
                } else {
                    if (containsLabel && nodePropertyEntry.key().equals(UUID_PROPERTY) && predicate.test(nodePropertyEntry)) {
                        nodePropertyEntry.entity().setProperty(UUID_PROPERTY, nodePropertyEntry.previouslyCommitedValue());
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

    private boolean containsLabel(String label, Iterable<Label> labelIterable) {
        return StreamSupport.stream(labelIterable.spliterator(), false).anyMatch(name -> name.name().equals(label));
    }

    public static Map<String, Map<String, Object>> list() {
        checkEnabled();
        updateUuid(null, null);
        return uuid;
    }

    private static void checkConstraintUuid(String label) {
        GraphDatabaseService db = properties.getGraphDatabase();
        try (Transaction tx = db.beginTx()) {
            Schema schema = db.schema();
            Stream<ConstraintDefinition> constraintDefinitionStream = StreamSupport.stream(schema.getConstraints(Label.label(label)).spliterator(), false);
            boolean exists = constraintDefinitionStream.anyMatch(constraint -> {
                Stream<String> streamPropertyKeys = StreamSupport.stream(constraint.getPropertyKeys().spliterator(), false);
                return streamPropertyKeys.anyMatch(property -> property.equals(UUID_PROPERTY));
            });
            if (!exists) {
                String error = String.format("`CREATE CONSTRAINT ON (%s:%s) ASSERT %s.uuid IS UNIQUE`", label.toLowerCase(), label, label.toLowerCase());
                throw new RuntimeException("No constraint found for label: " + label + ", please add the constraint with the following : " + error);
            }
            tx.success();
        }
    }

    public synchronized static Map<String, Object> remove(String label) {
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