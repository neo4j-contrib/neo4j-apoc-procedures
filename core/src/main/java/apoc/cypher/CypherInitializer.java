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
package apoc.cypher;

import apoc.ApocConfig;
import apoc.SystemLabels;
import apoc.util.LogsUtil;
import apoc.util.QueryUtil;
import apoc.util.Util;
import apoc.version.Version;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.SystemGraphComponent.Status;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.DatabaseEventListeners;
import org.neo4j.logging.Log;

import java.util.Collection;

import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;

import static apoc.SystemPropertyKeys.database;
import java.util.function.BooleanSupplier;


public class CypherInitializer implements AvailabilityListener {

    private final GraphDatabaseAPI db;
    private final Log userLog;
    private final GlobalProcedures procs;
    private final DependencyResolver dependencyResolver;
    private final String defaultDb;
    private final DatabaseManagementService databaseManagementService;
    private final DatabaseEventListeners databaseEventListeners;


    /**
     * indicates the status of the initializer, to be used for tests to ensure initializer operations are already done
     */
    private boolean finished = false;

    public CypherInitializer(GraphDatabaseAPI db, Log userLog,
                             DatabaseManagementService databaseManagementService,
                             DatabaseEventListeners databaseEventListeners) {
        this.db = db;
        this.userLog = userLog;
        this.databaseManagementService = databaseManagementService;
        this.databaseEventListeners = databaseEventListeners;
        this.dependencyResolver = db.getDependencyResolver();
        this.procs = dependencyResolver.resolveDependency(GlobalProcedures.class);
        this.defaultDb = dependencyResolver.resolveDependency(Config.class).get(GraphDatabaseSettings.default_database);
    }

    public boolean isFinished() {
        return finished;
    }

    public GraphDatabaseAPI getDb() {
        return db;
    }

    private boolean awaitUntil(BooleanSupplier supplier, int maxTime) {
        int time = 0;
        int sleep = 100;
        boolean condition = supplier.getAsBoolean();

        while (!condition && time < maxTime) {
            Util.sleep(sleep);
            time += sleep;
            condition = supplier.getAsBoolean();
        }

        return condition;
    }


    private void waitForSystemDbLifecycles() {
        /* Await 2 min at max for the system database to show in
           status CURRENT or REQUIRES_UPGRADE.
           We do not do anything for the other possible status because
           the dbms would not even be able to operate normally in them
           (i.e. UNSUPPORTED_BUT_CAN_UPGRADE, UNSUPPORTED, UNSUPPORTED_FUTURE).

           The reason for that is that the first time we initialize
           it the built-in roles (and possibly other system databases
           structures) will not be populated immediately in 4.4.
           I.e. system.isAvailable can output true but some of the
           Lifecycles may not have been run yet (UNINITIALIZED status)
         */
        boolean condition = awaitUntil( () ->
                                        {
                                            var components = dependencyResolver.resolveDependency( SystemGraphComponents.class );
                                            var status = components.detect( db );
                                            return (status == Status.CURRENT || status == Status.REQUIRES_UPGRADE);
                                        }, 120000 );

        if ( !condition )
        {
            userLog.warn("attempting to execute apoc.initializer without waiting for system database to finish initialization, waiting time elapsed. This may result in failing apoc.initializer commands." );        }
    }

    @Override
    public void available() {

        // run initializers in a new thread
        // we need to wait until apoc procs are registered
        // unfortunately an AvailabilityListener is triggered before that
        Util.newDaemonThread(() -> {
            try {
                final boolean isSystemDatabase = db.databaseName().equals(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
                Configuration config = dependencyResolver.resolveDependency(ApocConfig.class).getConfig();
                var initializers = collectInitializers(isSystemDatabase, config);

                // Wait for procedures to be loaded for the cypher initializer
                if (!isSystemDatabase) {
                    awaitApocProceduresRegistered();
                } else {
                    if (!initializers.isEmpty()) {
                        waitForSystemDbLifecycles();
                    }
                    // Register listener to drop apoc nodes from system database once a database is deleted
                    databaseEventListeners.registerDatabaseEventListener(new SystemFunctionalityListener());
                }

                if (defaultDb.equals(db.databaseName())) {
                    try {
                       String neo4jVersion = org.neo4j.kernel.internal.Version.getNeo4jVersion();
                        final String apocFullVersion = Version.class.getPackage().getImplementationVersion();
                        if (isVersionDifferent(neo4jVersion, apocFullVersion)) {
                            userLog.warn("The apoc version (%s) and the Neo4j DBMS versions %s are incompatible. \n" +
                                            "See the compatibility matrix in https://neo4j.com/labs/apoc/4.4/installation/ to see the correct version",
                                    apocFullVersion, neo4jVersion);
                        }
                    } catch (Exception ignored) {
                        userLog.info("Cannot check APOC version compatibility because of a transient error. Retrying your request at a later time may succeed");
                    }
                }

                for (String query : initializers) {
                    if (QueryUtil.isValidQuery(query)) {
                        String sanitizedQuery = LogsUtil.sanitizeQuery(query);
                        try {
                            // we need to apply a retry strategy here since in systemdb we potentially conflict with
                            // creating constraints which could cause our query to fail with a transient error.
                            Util.retryInTx(userLog, db, tx -> Iterators.count(tx.execute(query)), 0, 5, retries -> { });
                            userLog.info("successfully initialized: " + sanitizedQuery);
                        } catch (Exception e) {
                            userLog.error("error upon initialization, running: " + sanitizedQuery, e);
                        }
                    } else {
                        userLog.error("error upon initialization, invalid query: " + query);
                    }
                }
            } finally {
                finished = true;
            }
        }).start();
    }

    // the visibility is public only for testing purpose, it could be private otherwise
    public static boolean isVersionDifferent(String neo4jVersion, String apocVersion) {
        final String[] apocSplit = splitVersion(apocVersion);
        final String[] neo4jSplit = splitVersion(neo4jVersion);

        return !(apocSplit != null && neo4jSplit != null
                && apocSplit[0].equals(neo4jSplit[0])
                && apocSplit[1].equals(neo4jSplit[1]));
    }

    private static String[] splitVersion(String completeVersion) {
        if (StringUtils.isBlank(completeVersion)) {
            return null;
        }
        return completeVersion.split("[^\\d]");
    }

    private Collection<String> collectInitializers(boolean isSystemDatabase, Configuration config) {
        Map<String, String> initializers = new TreeMap<>();

        config.getKeys(ApocConfig.APOC_CONFIG_INITIALIZER + "." + db.databaseName())
                .forEachRemaining(key -> putIfNotBlank(initializers, key, config.getString(key)));

        // add legacy style initializers
        if (!isSystemDatabase) {
            config.getKeys(ApocConfig.APOC_CONFIG_INITIALIZER_CYPHER)
                    .forEachRemaining(key -> initializers.put(key, config.getString(key)));
        }

        return initializers.values();
    }

    private void putIfNotBlank(Map<String,String> map, String key, String value) {
        if ((value!=null) && (!value.isBlank())) {
            map.put(key, value);
        }
    }

    private void awaitApocProceduresRegistered() {
        while (!areProceduresRegistered("apoc")) {
            Util.sleep(100);
        }
    }

    private boolean areProceduresRegistered(String procStart) {
        try {
            return procs.getAllProcedures().stream().anyMatch(signature -> signature.name().toString().startsWith(procStart));
        } catch (ConcurrentModificationException e) {
            // if a CME happens (possible during procedure scanning)
            // we return false and the caller will try again
            return false;
        }
    }

    @Override
    public void unavailable() {
        // intentionally empty
    }

    private class SystemFunctionalityListener implements DatabaseEventListener {

        @Override
        public void databaseDrop(DatabaseEventContext eventContext) {

            forEachSystemLabel((tx, label) -> {
                tx.findNodes(label, database.name(), eventContext.getDatabaseName())
                        .forEachRemaining(Node::delete);
            });
        }

        @Override
        public void databaseStart(DatabaseEventContext eventContext) {}

        @Override
        public void databaseShutdown(DatabaseEventContext eventContext) {}

        @Override
        public void databasePanic(DatabaseEventContext eventContext) {}

        @Override
        public void databaseCreate(DatabaseEventContext eventContext) {}
    }

    private void forEachSystemLabel(BiConsumer<Transaction, Label> consumer) {
        try (Transaction tx = db.beginTx()) {
            for (Label label: SystemLabels.values()) {
                consumer.accept(tx, label);
            }
            tx.commit();
        }
    }
}
