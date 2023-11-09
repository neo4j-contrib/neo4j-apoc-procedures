/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.uuid;

import apoc.ApocConfig;
import apoc.Pools;
import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.Util;
import org.apache.commons.collections4.IterableUtils;
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
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.ApocConfig.APOC_UUID_FORMAT;
import static apoc.util.SystemDbUtil.getLastUpdate;
import static apoc.uuid.Uuid.setExistingNodes;
import static apoc.uuid.UuidConfig.*;

public class UuidHandler extends LifecycleAdapter implements TransactionEventListener<Void> {

    private final GraphDatabaseAPI db;
    private final Log log;
    private final DatabaseManagementService databaseManagementService;
    private final ApocConfig apocConfig;
    // Snapshot of uuid configuration per label/property.
    // The containing map is immutable!
    private final AtomicReference<Map<String, UuidConfig>> labelAndPropertyNamesSnapshot = new AtomicReference<>(Map.of());
    private final ApocConfig.UuidFormatType uuidFormat;
    private final JobScheduler jobScheduler;
    private final Pools pools;

    private JobHandle refreshUuidHandle;
    private volatile long lastUpdate;

    public static final String APOC_UUID_REFRESH = "apoc.uuid.refresh";

    public static final String NOT_ENABLED_ERROR = "UUID have not been enabled." +
            " Set 'apoc.uuid.enabled=true' or 'apoc.uuid.enabled.%s=true' in your apoc.conf file located in the $NEO4J_HOME/conf/ directory.";

    public UuidHandler(GraphDatabaseAPI db, DatabaseManagementService databaseManagementService, Log log, ApocConfig apocConfig, JobScheduler jobScheduler,
                       Pools pools) {
        this.db = db;
        this.databaseManagementService = databaseManagementService;
        this.log = log;
        this.apocConfig = apocConfig;
        this.uuidFormat = apocConfig.getEnumProperty(APOC_UUID_FORMAT, ApocConfig.UuidFormatType.class, ApocConfig.UuidFormatType.hex);
        this.jobScheduler = jobScheduler;
        this.pools = pools;
    }

    @Override
    public void start() {
        if (isEnabled()) {
            refresh();
            // not to cause breaking-change, with deprecated procedures we don't schedule the refresh()
            Integer uuidRefresh = apocConfig.getConfig().getInteger(APOC_UUID_REFRESH, null);
            if (uuidRefresh != null) {
                refreshUuidHandle = jobScheduler.scheduleRecurring(Group.STORAGE_MAINTENANCE, () -> {
                            if (getLastUpdate(db.databaseName(), SystemLabels.ApocUuidMeta) >= lastUpdate) {
                                refreshAndAdd();
                            }
                        },
                        uuidRefresh, uuidRefresh, TimeUnit.MILLISECONDS);
            }

            databaseManagementService.registerTransactionEventListener(db.databaseName(), this);
        }
    }

    private boolean isEnabled() {
        return UUIDHandlerNewProcedures.isEnabled(this.db.databaseName());
    }

    @Override
    public void stop() {
        if (isEnabled()) {
            databaseManagementService.unregisterTransactionEventListener(db.databaseName(), this);

            if (refreshUuidHandle != null) {
                refreshUuidHandle.cancel();
            }
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
    public Void beforeCommit(TransactionData txData, Transaction transaction, GraphDatabaseService databaseService) {

        // assignedLabels handles both created nodes and set labels of existing nodes
        Iterable<PropertyEntry<Node>> assignedNodeProperties = txData.assignedNodeProperties();
        Iterable<PropertyEntry<Node>> removedNodeProperties = txData.removedNodeProperties();

        labelAndPropertyNamesSnapshot.get().forEach((label, config) -> {
            final String propertyName = config.getUuidProperty();
            List<Node> nodes = config.isAddToSetLabels()
                    ? StreamSupport.stream(txData.assignedLabels().spliterator(), false).map(LabelEntry::node).collect(Collectors.toList())
                    : IterableUtils.toList(txData.createdNodes());
            try {
                nodes.forEach(node -> {
                    if (node.hasLabel(Label.label(label)) && !node.hasProperty(propertyName)) {
                        String uuid = generateUuidValue();
                        node.setProperty(propertyName, uuid);
                    }
                });
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
        UUIDHandlerNewProcedures.checkEnabled(db.databaseName());
    }

    private String generateUuidValue() {
        UUID uuid = UUID.randomUUID();
        switch (uuidFormat) {
            case base64:
                return UuidUtil.generateBase64Uuid(uuid);
            case hex:
            default:
                return uuid.toString();
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
            String error = String.format("`CREATE CONSTRAINT FOR (%s:%s) REQUIRE %s.%s IS UNIQUE`",
                    label.toLowerCase(), label, label.toLowerCase(), propertyName);
            throw new RuntimeException("No constraint found for label: " + label + ", please add the constraint with the following : " + error);
        }
    }

    public void add(Transaction tx, String label, UuidConfig config) {
        checkEnabled();
        final String propertyName = config.getUuidProperty();
        checkConstraintUuid(tx, label, propertyName);

        try (Transaction sysTx = apocConfig.getSystemDb().beginTx()) {
            Node node = Util.mergeNode(sysTx, SystemLabels.ApocUuid, null,
                    Pair.of(SystemPropertyKeys.database.name(), db.databaseName()),
                    Pair.of(SystemPropertyKeys.label.name(), label),
                    Pair.of(SystemPropertyKeys.propertyName.name(), propertyName)
                    );
            node.setProperty(SystemPropertyKeys.addToSetLabel.name(), config.isAddToSetLabels());
            sysTx.commit();
        }
        refresh();
    }

    public Map<String, UuidConfig> list() {
        checkEnabled();
        return labelAndPropertyNamesSnapshot.get();
    }

    public synchronized void refreshAndAdd() {
        final var start = System.currentTimeMillis();
        final var localCache = provisionalRefresh();

        if (Util.isWriteableInstance(db)) {
            // add to existing nodes
            localCache.forEach((label, conf) -> {
                // auto-create uuid constraint
                if (conf.isCreateConstraint()) {
                    String queryConst = String.format("CREATE CONSTRAINT IF NOT EXISTS FOR (n:%s) REQUIRE (n.%s) IS UNIQUE",
                            Util.quote(label),
                            Util.quote(conf.getUuidProperty())
                    );
                    db.executeTransactionally(queryConst);
                    conf.setCreateConstraint(false);
                }

                if (conf.isAddToExistingNodes()) {
                    Map<String, Object> result = setExistingNodes(db, pools, label, conf);

                    String logBatchResult = String.format(
                            "Result of batch computation obtained from existing nodes for UUID handler with label `%s`: \n %s",
                            label, result);
                    log.info(logBatchResult);
                    conf.setAddToExistingNodes(false);
                }
            });
        }

        labelAndPropertyNamesSnapshot.set(localCache);
        lastUpdate = start;
    }

    public Map<String, UuidConfig> provisionalRefresh() {
        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
            return tx.findNodes(SystemLabels.ApocUuid, SystemPropertyKeys.database.name(), db.databaseName())
                    .stream()
                    .collect(Collectors.toUnmodifiableMap(
                        node -> (String) node.getProperty(SystemPropertyKeys.label.name()),
                        node -> new UuidConfig(Map.of(
                                UUID_PROPERTY_KEY, node.getProperty(SystemPropertyKeys.propertyName.name()),
                                ADD_TO_SET_LABELS_KEY, node.getProperty(SystemPropertyKeys.addToSetLabel.name(), false),
                                ADD_TO_EXISTING_NODES_KEY, node.getProperty(SystemPropertyKeys.addToExistingNodes.name(), false)
                        ))
                    ));
        }
    }

    public void refresh() {
        final var start = System.currentTimeMillis();
        // Note, there is a race condition here where lastUpdate can become larger than the last update
        // if two threads are refreshing at the same time.
        labelAndPropertyNamesSnapshot.set(provisionalRefresh());
        lastUpdate = start;
    }

    public synchronized UuidConfig remove(String label) {
        final var oldValue = labelAndPropertyNamesSnapshot.get().get(label);
        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
            tx.findNodes(SystemLabels.ApocUuid, SystemPropertyKeys.database.name(), db.databaseName(),
                    SystemPropertyKeys.label.name(), label)
                    .forEachRemaining(node -> node.delete());
            tx.commit();
        }
        refresh();
        return oldValue;
    }

    public synchronized Map<String, UuidConfig> removeAll() {
        final var retval = labelAndPropertyNamesSnapshot.get();
        labelAndPropertyNamesSnapshot.set(Map.of());
        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
            tx.findNodes(SystemLabels.ApocUuid, SystemPropertyKeys.database.name(), db.databaseName() )
                    .forEachRemaining(node -> node.delete());
            tx.commit();
        }
        return retval;
    }

}
