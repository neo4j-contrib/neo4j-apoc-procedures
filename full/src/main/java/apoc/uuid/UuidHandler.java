package apoc.uuid;

import apoc.ApocConfig;
import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.Util;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.ApocConfig.APOC_UUID_ENABLED;

public class UuidHandler extends LifecycleAdapter implements TransactionEventListener<Void> {

    private final GraphDatabaseAPI db;
    private final Log log;
    private final DatabaseManagementService databaseManagementService;
    private final ApocConfig apocConfig;
    private final ConcurrentHashMap<String, String> configuredLabelAndPropertyNames = new ConcurrentHashMap<>();

    public static final String NOT_ENABLED_ERROR = "UUID have not been enabled." +
            " Set 'apoc.uuid.enabled=true' or 'apoc.uuid.enabled.%s=true' in your apoc.conf file located in the $NEO4J_HOME/conf/ directory.";

    public UuidHandler(GraphDatabaseAPI db, DatabaseManagementService databaseManagementService, Log log, ApocConfig apocConfig, GlobalProcedures globalProceduresRegistry) {
        this.db = db;
        this.databaseManagementService = databaseManagementService;
        this.log = log;
        this.apocConfig = apocConfig;
    }

    @Override
    public void start() {
        if (isEnabled()) {
            refresh();
            databaseManagementService.registerTransactionEventListener(db.databaseName(), this);
        }
    }

    private boolean isEnabled() {
        String apocUUIDEnabledDb = String.format(ApocConfig.APOC_UUID_ENABLED_DB, this.db.databaseName());
        return apocConfig.getConfig().getBoolean(apocUUIDEnabledDb, apocConfig.getBoolean(APOC_UUID_ENABLED));
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
    public Void beforeCommit(TransactionData txData, Transaction transaction, GraphDatabaseService databaseService) throws Exception {

        // assignedLabels handles both created nodes and set labels of existing nodes
        Iterable<LabelEntry> assignedLabels = txData.assignedLabels();
        Iterable<PropertyEntry<Node>> assignedNodeProperties = txData.assignedNodeProperties();
        Iterable<PropertyEntry<Node>> removedNodeProperties = txData.removedNodeProperties();

        configuredLabelAndPropertyNames.forEach((label, propertyName) -> {
            try {
                if (assignedLabels.iterator().hasNext()) {
                    assignedLabels.forEach(labelEntry -> {
                        Node node = labelEntry.node();
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
            throw new RuntimeException(String.format(NOT_ENABLED_ERROR, this.db.databaseName()) );
        }
    }

    public void checkConstraintUuid(Transaction tx, String label, String propertyName) {
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

    public void add(Transaction tx, String label, String propertyName) {
        checkEnabled();
        checkConstraintUuid(tx, label, propertyName);

        configuredLabelAndPropertyNames.put(label, propertyName);

        try (Transaction sysTx = apocConfig.getSystemDb().beginTx()) {
            Node node = Util.mergeNode(sysTx, SystemLabels.ApocUuid, null,
                    Pair.of(SystemPropertyKeys.database.name(), db.databaseName()),
                    Pair.of(SystemPropertyKeys.label.name(), label),
                    Pair.of(SystemPropertyKeys.propertyName.name(), propertyName)
                    );
            sysTx.commit();
        }
    }

    public Map<String, String> list() {
        checkEnabled();
        return configuredLabelAndPropertyNames;
    }

    public void refresh() {
        configuredLabelAndPropertyNames.clear();
        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
            tx.findNodes(SystemLabels.ApocUuid, SystemPropertyKeys.database.name(), db.databaseName())
                    .forEachRemaining(node ->
                            configuredLabelAndPropertyNames.put(
                                    (String)node.getProperty(SystemPropertyKeys.label.name()),
                                    (String)node.getProperty(SystemPropertyKeys.propertyName.name())));
            tx.commit();
        }
    }

    public synchronized String remove(String label) {
        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
            tx.findNodes(SystemLabels.ApocUuid, SystemPropertyKeys.database.name(), db.databaseName(),
                    SystemPropertyKeys.label.name(), label)
                    .forEachRemaining(node -> node.delete());
            tx.commit();
        }
        return configuredLabelAndPropertyNames.remove(label);
    }

    public synchronized Map<String, String> removeAll() {
        Map<String, String> retval = new HashMap<>(configuredLabelAndPropertyNames);
        configuredLabelAndPropertyNames.clear();
        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
            tx.findNodes(SystemLabels.ApocUuid, SystemPropertyKeys.database.name(), db.databaseName() )
                    .forEachRemaining(node -> node.delete());
            tx.commit();
        }
        return retval;
    }

}
