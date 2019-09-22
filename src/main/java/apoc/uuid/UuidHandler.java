package apoc.uuid;

import apoc.ApocConfig;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResultConsumer;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.ApocConfig.APOC_UUID_ENABLED;

public class UuidHandler extends LifecycleAdapter implements TransactionEventListener<Void> {

    private final GraphDatabaseAPI db;
    private final Log log;
    private final DatabaseManagementService databaseManagementService;
    private final ApocConfig apocConfig;
    private final ConcurrentHashMap<String, String> configuredLabelAndPropertyNames = new ConcurrentHashMap<>();

    private static final String NOT_ENABLED_ERROR = "UUID have not been enabled." +
            " Set 'apoc.uuid.enabled=true' in your apoc.conf file located in the $NEO4J_HOME/conf/ directory.";

    private static final Map<String, UuidHandler> uuidLifecyclesByDatabaseName = new ConcurrentHashMap<>();

    public UuidHandler(GraphDatabaseAPI db, DatabaseManagementService databaseManagementService, Log log, ApocConfig apocConfig, GlobalProceduresRegistry globalProceduresRegistry) {
        this.db = db;
        this.databaseManagementService = databaseManagementService;
        this.log = log;
        this.apocConfig = apocConfig;
        uuidLifecyclesByDatabaseName.put(db.databaseName(), this);
        globalProceduresRegistry.registerComponent(UuidHandler.class, ctx -> {
            String databaseName = ctx.graphDatabaseAPI().databaseName();
            return uuidLifecyclesByDatabaseName.get(databaseName);
        }, true);
    }

    @Override
    public void start() {
        if (isEnabled()) {
            refresh();
            databaseManagementService.registerTransactionEventListener(db.databaseName(), this);
        }
    }

    private boolean isEnabled() {
        return apocConfig.getBoolean(APOC_UUID_ENABLED);
    }

    @Override
    public void stop() {
        if (isEnabled()) {
            databaseManagementService.unregisterTransactionEventListener(db.databaseName(), this);
        }
    }

    private void checkAndRestoreUuidProperty(Iterable<PropertyEntry<Node>> nodeProperties, String label, String uuidProperty) {
        checkAndRestoreUuidProperty(nodeProperties, label, uuidProperty, null);
    }

    private void checkAndRestoreUuidProperty(Iterable<PropertyEntry<Node>> nodeProperties, String label, String uuidProperty, Predicate<PropertyEntry<Node>> predicate) {
        if (nodeProperties.iterator().hasNext()) {
            nodeProperties.forEach(nodePropertyEntry -> {
                if (predicate == null) {
                    if (nodePropertyEntry.entity().hasLabel(Label.label(label)) && nodePropertyEntry.key().equals(uuidProperty)) {
                        nodePropertyEntry.entity().setProperty(uuidProperty, nodePropertyEntry.previouslyCommittedValue());
                    }
                } else {
                    if (nodePropertyEntry.entity().hasLabel(Label.label(label)) && nodePropertyEntry.key().equals(uuidProperty) && predicate.test(nodePropertyEntry)) {
                        nodePropertyEntry.entity().setProperty(uuidProperty, nodePropertyEntry.previouslyCommittedValue());
                    }
                }
            });
        }
    }

    @Override
    public Void beforeCommit(TransactionData txData, GraphDatabaseService databaseService) throws Exception {

        Iterable<Node> createdNodes = txData.createdNodes();
        Iterable<PropertyEntry<Node>> assignedNodeProperties = txData.assignedNodeProperties();
        Iterable<PropertyEntry<Node>> removedNodeProperties = txData.removedNodeProperties();

        configuredLabelAndPropertyNames.forEach((label, propertyName) -> {
            try {
                if (createdNodes.iterator().hasNext()) {
                    createdNodes.forEach(node -> {
                        if (node.hasLabel(Label.label(label)) && !node.hasProperty(propertyName)) {
                            String uuid = UUID.randomUUID().toString();
                            node.setProperty(propertyName, uuid);
                        }
                    });
                }
                checkAndRestoreUuidProperty(assignedNodeProperties, label, propertyName,
                        (nodePropertyEntry) -> nodePropertyEntry.value() == null || nodePropertyEntry.value().equals(""));
                checkAndRestoreUuidProperty(removedNodeProperties, label, propertyName);
            } catch (Exception e) {
                log.warn("Error executing uuid " + label + " in phase before", e);
            }
        });
        return null;
    }

    @Override
    public void afterCommit(TransactionData data, Void state, GraphDatabaseService databaseService) {

    }

    @Override
    public void afterRollback(TransactionData data, Void state, GraphDatabaseService databaseService) {

    }


    private void checkEnabled() {
        if (!isEnabled()) {
            throw new RuntimeException(NOT_ENABLED_ERROR);
        }
    }

    public void checkConstraintUuid(String label, String propertyName) {
        Schema schema = db.schema();
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

    public void add(String label, String propertyName) {
        checkEnabled();
        checkConstraintUuid(label, propertyName);

        configuredLabelAndPropertyNames.put(label, propertyName);

        Map<String, Object> params = MapUtil.map("label", label, "propertyName", propertyName);
        apocConfig.getSystemDb().executeTransactionally(
                String.format("merge (node:ApocUuid:%s{label: $label, propertyName: $propertyName})", db.databaseName()),
                params,
                ResultConsumer.EMPTY_CONSUMER);
    }

    public Map<String, String> list() {
        checkEnabled();
        return configuredLabelAndPropertyNames;
    }

    public void refresh() {
        configuredLabelAndPropertyNames.clear();
        apocConfig.getSystemDb().executeTransactionally(
                String.format("match (node:ApocUuid:%s) return node.label as label, node.propertyName as propertyName", db.databaseName()),
                null,
                result -> configuredLabelAndPropertyNames.putAll(result.stream().collect(Collectors.toMap(
                        m -> (String) (m.get("label")),
                        m -> (String) (m.get("propertyName"))
                        )
                    )
                )
        );
    }

    public synchronized String remove(String label) {
        apocConfig.getSystemDb().executeTransactionally(
                String.format("match (node:ApocUuid:%s{label: $label}) delete node", db.databaseName()),
                Collections.singletonMap("label", label),
                ResultConsumer.EMPTY_CONSUMER
        );
        return configuredLabelAndPropertyNames.remove(label);
    }

    public synchronized Map<String, String> removeAll() {
        Map<String, String> retval = new HashMap<>(configuredLabelAndPropertyNames);
        configuredLabelAndPropertyNames.clear();
        apocConfig.getSystemDb().executeTransactionally(
                String.format("match (node:ApocUuid:%s) delete node", db.databaseName())
        );
        return retval;
    }


}
