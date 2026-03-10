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
package org.neo4j.test.rule;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.Neo4jDatabaseManagementServiceBuilder;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.connectioninfo.RoutingInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.TransactionExceptionMapper;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public abstract class DbmsRule extends ExternalResource implements GraphDatabaseAPI {
    private Neo4jDatabaseManagementServiceBuilder databaseBuilder;
    private GraphDatabaseAPI database;
    private boolean startEagerly = true;
    private final Map<Setting<?>, Object> globalConfig = new HashMap<>();
    private DatabaseManagementService managementService;

    /**
     * Means the database will be started on first {@link #getGraphDatabaseAPI()}}
     * or {@link #ensureStarted()} call.
     */
    public DbmsRule startLazily() {
        startEagerly = false;
        return this;
    }

    @Override
    public void executeTransactionally(String query) throws QueryExecutionException {
        getGraphDatabaseAPI().executeTransactionally(query);
    }

    @Override
    public void executeTransactionally(String query, Map<String, Object> parameters) throws QueryExecutionException {
        getGraphDatabaseAPI().executeTransactionally(query, parameters);
    }

    @Override
    public <T> T executeTransactionally(
            String query, Map<String, Object> parameters, ResultTransformer<T> resultTransformer)
            throws QueryExecutionException {
        return getGraphDatabaseAPI().executeTransactionally(query, parameters, resultTransformer);
    }

    @Override
    public <T> T executeTransactionally(
            String query, Map<String, Object> parameters, ResultTransformer<T> resultTransformer, Duration timeout)
            throws QueryExecutionException {
        return getGraphDatabaseAPI().executeTransactionally(query, parameters, resultTransformer, timeout);
    }

    @Override
    public InternalTransaction beginTransaction(KernelTransaction.Type type, LoginContext loginContext) {
        return getGraphDatabaseAPI().beginTransaction(type, loginContext);
    }

    @Override
    public InternalTransaction beginTransaction(
            KernelTransaction.Type type, LoginContext loginContext, ClientConnectionInfo connectionInfo) {
        return getGraphDatabaseAPI().beginTransaction(type, loginContext, connectionInfo);
    }

    @Override
    public InternalTransaction beginTransaction(
            KernelTransaction.Type type,
            LoginContext loginContext,
            ClientConnectionInfo connectionInfo,
            long timeout,
            TimeUnit unit) {
        return getGraphDatabaseAPI().beginTransaction(type, loginContext, connectionInfo, timeout, unit);
    }

    @Override
    public InternalTransaction beginTransaction(
            KernelTransaction.Type type,
            LoginContext loginContext,
            ClientConnectionInfo clientInfo,
            RoutingInfo routingInfo,
            List<String> bookmarks,
            long timeout,
            TimeUnit unit,
            Consumer<Status> terminationCallback,
            TransactionExceptionMapper transactionExceptionMapper) {
        return getGraphDatabaseAPI()
                .beginTransaction(
                        type,
                        loginContext,
                        clientInfo,
                        routingInfo,
                        bookmarks,
                        timeout,
                        unit,
                        terminationCallback,
                        transactionExceptionMapper);
    }

    @Override
    public Transaction beginTx() {
        return getGraphDatabaseAPI().beginTx();
    }

    @Override
    public Transaction beginTx(long timeout, TimeUnit timeUnit) {
        return getGraphDatabaseAPI().beginTx(timeout, timeUnit);
    }

    @Override
    protected void before() {
        create();
        if (startEagerly) {
            ensureStarted();
        }
    }

    @Override
    protected void after() {
        shutdown();
    }

    private void create() {
        databaseBuilder = newFactory();

        // Allow experimental versions of Cypher and set Cypher Default Version
        // Might need to be enabled when the next experimental version appear:
        // globalConfig.put(GraphDatabaseInternalSettings.enable_experimental_cypher_versions, true);

        // A test may set this for the entire file itself, so we shouldn't override that
        if (!globalConfig.containsKey(GraphDatabaseSettings.default_language)) {
            String cypherVersionEnv =
                    System.getenv().getOrDefault("CYPHER_VERSION", GraphDatabaseSettings.CypherVersion.Cypher5.name());
            GraphDatabaseSettings.CypherVersion cypherVersion =
                    GraphDatabaseSettings.CypherVersion.valueOf(cypherVersionEnv);
            globalConfig.put(GraphDatabaseSettings.default_language, cypherVersion);
        }

        databaseBuilder.setConfig(globalConfig);
    }

    protected abstract Neo4jDatabaseManagementServiceBuilder newFactory();

    /**
     * {@link DbmsRule} now implements {@link GraphDatabaseAPI} directly, so no need. Also for ensuring
     * a lazily started database is created, use {@link #ensureStarted()} instead.
     */
    public GraphDatabaseAPI getGraphDatabaseAPI() {
        ensureStarted();
        return database;
    }

    public DatabaseManagementService getManagementService() {
        return managementService;
    }

    public synchronized void ensureStarted() {
        if (database == null) {
            managementService = databaseBuilder.build();
            database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        }
    }

    /**
     * Adds or replaces a setting for the database managed by this database rule.
     * <p>
     * If this method is called when constructing the rule, the setting is considered a global setting applied to all tests.
     * <p>
     * If this method is called inside a specific test, i.e. after {@link #before()}, but before started (a call to {@link #startLazily()} have been made),
     * then this setting will be considered a test-specific setting, adding to or overriding the global settings for this test only.
     * Test-specific settings will be remembered throughout a test, even between restarts.
     * <p>
     * If this method is called when a database is already started an {@link IllegalStateException} will be thrown since the setting
     * will have no effect, instead letting the developer notice that and change the test code.
     */
    public <T> DbmsRule withSetting(Setting<T> key, T value) {
        if (database != null) {
            // Database already started
            throw new IllegalStateException(
                    "Wanted to set " + key + "=" + value + ", but database has already been started");
        }
        if (databaseBuilder != null) {
            // Test already started, but db not yet started
            databaseBuilder.setConfig(key, value);
        } else {
            // Test haven't started, we're still in phase of constructing this rule
            globalConfig.put(key, value);
        }
        return this;
    }

    public void restartDatabase(Map<Setting<?>, Object> configChanges) {
        managementService.shutdown();
        database = null;
        // This DatabaseBuilder has already been configured with the global settings as well as any test-specific
        // settings,
        // so just apply these additional settings.
        databaseBuilder.setConfig(configChanges);
        getGraphDatabaseAPI();
    }

    public void shutdown() {
        try {
            if (managementService != null) {
                managementService.shutdown();
            }
        } finally {
            managementService = null;
            database = null;
        }
    }

    @Override
    public NamedDatabaseId databaseId() {
        return database.databaseId();
    }

    @Override
    public DbmsInfo dbmsInfo() {
        return database.dbmsInfo();
    }

    @Override
    public HostedOnMode mode() {
        return database.mode();
    }

    @Override
    public DependencyResolver getDependencyResolver() {
        return database.getDependencyResolver();
    }

    @Override
    public DatabaseLayout databaseLayout() {
        return database.databaseLayout();
    }

    @Override
    public boolean isAvailable(long timeout) {
        return database.isAvailable(timeout);
    }

    @Override
    public boolean isAvailable() {
        return database.isAvailable();
    }

    @Override
    public String databaseName() {
        return database.databaseName();
    }
}