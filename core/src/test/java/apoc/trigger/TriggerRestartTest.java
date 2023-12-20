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
package apoc.trigger;

import static apoc.trigger.TriggerTestUtil.TRIGGER_DEFAULT_REFRESH;
import static apoc.trigger.TriggerTestUtil.awaitTriggerDiscovered;
import static apoc.util.TestUtil.waitDbsAvailable;
import static org.junit.Assert.assertEquals;

import apoc.ApocConfig;
import apoc.util.TestUtil;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class TriggerRestartTest {

    @Rule
    public TemporaryFolder store_dir = new TemporaryFolder();

    private GraphDatabaseService db;
    private GraphDatabaseService sysDb;
    private DatabaseManagementService databaseManagementService;

    // we cannot set via apocConfig().setProperty(apoc.trigger.refresh, ...) in `@Before`, because is too late
    @ClassRule
    public static final ProvideSystemProperty systemPropertyRule =
            new ProvideSystemProperty("apoc.trigger.refresh", String.valueOf(TRIGGER_DEFAULT_REFRESH));

    @Before
    public void setUp() throws IOException {
        databaseManagementService =
                new TestDatabaseManagementServiceBuilder(store_dir.getRoot().toPath()).build();
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        sysDb = databaseManagementService.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
        waitDbsAvailable(db, sysDb);
        ApocConfig.apocConfig().setProperty("apoc.trigger.enabled", "true");
        TestUtil.registerProcedure(db, TriggerNewProcedures.class, Trigger.class);
    }

    @After
    public void tearDown() {
        databaseManagementService.shutdown();
    }

    private void restartDb() {
        databaseManagementService.shutdown();
        databaseManagementService =
                new TestDatabaseManagementServiceBuilder(store_dir.getRoot().toPath()).build();
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        sysDb = databaseManagementService.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
        waitDbsAvailable(db, sysDb);
    }

    @Test
    public void testTriggerRunsAfterRestart() throws Exception {
        final String query =
                "CALL apoc.trigger.add('myTrigger', 'unwind $createdNodes as n set n.trigger = n.trigger + 1', {phase:'before'})";
        testTriggerWorksBeforeAndAfterRestart(db, query, Collections.emptyMap(), () -> {});
    }

    @Test
    public void testTriggerViaInstallRunsAfterRestart() {
        final String name = "myTrigger";
        final String innerQuery = "unwind $createdNodes as n set n.trigger = n.trigger + 1";
        final Map<String, Object> params = Map.of("name", name, "query", innerQuery);
        final String triggerQuery =
                "CALL apoc.trigger.install('neo4j', 'myTrigger', 'unwind $createdNodes as n set n.trigger = n.trigger + 1', {phase:'before'})";
        testTriggerWorksBeforeAndAfterRestart(
                sysDb, triggerQuery, params, () -> awaitTriggerDiscovered(db, name, innerQuery));
    }

    @Test
    public void testTriggerViaBothAddAndInstall() {
        // executing both trigger add and install with the same name will not duplicate the eventListeners
        final String name = "myTrigger";
        final String innerQuery = "unwind $createdNodes as n set n.trigger = n.trigger + 1";

        final String triggerQuery = "CALL apoc.trigger.add($name, $query, {phase:'before'})";

        final Map<String, Object> params = Map.of("name", name, "query", innerQuery);

        final Runnable runnable = () -> {
            sysDb.executeTransactionally("CALL apoc.trigger.install('neo4j', $name, $query, {phase:'before'})", params);
            awaitTriggerDiscovered(db, name, innerQuery);
        };
        testTriggerWorksBeforeAndAfterRestart(db, triggerQuery, params, runnable);
    }

    private void testTriggerWorksBeforeAndAfterRestart(
            GraphDatabaseService gbs, String query, Map<String, Object> params, Runnable runnable) {
        TestUtil.testCall(gbs, query, params, row -> {});
        runnable.run();

        db.executeTransactionally("CREATE (p:Person{id:1, trigger: 0})");
        TestUtil.testCall(
                db, "match (n:Person{id:1}) return n.trigger as trigger", r -> assertEquals(1L, r.get("trigger")));

        restartDb();

        db.executeTransactionally("CREATE (p:Person{id:2, trigger: 0})");
        TestUtil.testCall(
                db, "match (n:Person{id:1}) return n.trigger as trigger", r -> assertEquals(1L, r.get("trigger")));
        TestUtil.testCall(
                db, "match (n:Person{id:2}) return n.trigger as trigger", r -> assertEquals(1L, r.get("trigger")));
    }
}
