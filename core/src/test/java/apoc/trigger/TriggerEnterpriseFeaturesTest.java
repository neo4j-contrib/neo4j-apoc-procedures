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

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static apoc.ApocConfig.APOC_TRIGGER_ENABLED;
import static apoc.trigger.TriggerHandler.TRIGGER_REFRESH;
import static apoc.trigger.TriggerTestUtil.TIMEOUT;
import static apoc.trigger.TriggerTestUtil.TRIGGER_DEFAULT_REFRESH;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;
import static apoc.util.TestContainerUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.driver.SessionConfig.forDatabase;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class TriggerEnterpriseFeaturesTest {
    private static final String FOO_DB = "foo";

    private static final String NO_ADMIN_USER = "nonadmin";
    private static final String NO_ADMIN_PWD = "test1234";

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeAll() {
        // We build the project, the artifact will be placed into ./build/libs
        neo4jContainer = createEnterpriseDB(!TestUtil.isRunningInCI())
                .withEnv(APOC_TRIGGER_ENABLED, "true")
                .withEnv(TRIGGER_REFRESH, String.valueOf(TRIGGER_DEFAULT_REFRESH));
        neo4jContainer.start();
        session = neo4jContainer.getSession();

        assertTrue(neo4jContainer.isRunning());

        try (Session sysSession = neo4jContainer.getDriver().session(forDatabase(SYSTEM_DATABASE_NAME))) {
            sysSession.writeTransaction(tx -> tx.run(String.format("CREATE DATABASE %s WAIT;", FOO_DB)));

            sysSession.run(String.format("CREATE USER %s SET PASSWORD '%s' SET PASSWORD CHANGE NOT REQUIRED",
                    NO_ADMIN_USER, NO_ADMIN_PWD));
        }
    }

    @AfterClass
    public static void afterAll() {
        session.close();
        neo4jContainer.close();
    }

    @After
    public void after() {
        // drop all triggers
        try (Session sysSession = neo4jContainer.getDriver().session(forDatabase(SYSTEM_DATABASE_NAME))) {
            Stream.of(DEFAULT_DATABASE_NAME, FOO_DB)
                    .forEach(dbName -> sysSession.run( "call apoc.trigger.dropAll($dbName)",
                            Map.of("dbName", dbName) )
                    );
        }
    }


    @Test
    public void testTriggerShowInCorrectDatabase() {
        final String defaultTriggerName = UUID.randomUUID().toString();
        final String fooTriggerName = UUID.randomUUID().toString();

        try (Session sysSession = neo4jContainer.getDriver().session(forDatabase(SYSTEM_DATABASE_NAME))) {
            // install and show in default db
            testCall(sysSession, "CALL apoc.trigger.install($dbName, $name, 'return 1', {})",
                    Map.of("dbName", DEFAULT_DATABASE_NAME, "name", defaultTriggerName),
                    r -> assertEquals(defaultTriggerName, r.get("name"))
            );

            testCall(sysSession, "CALL apoc.trigger.show($dbName)",
                    Map.of("dbName", DEFAULT_DATABASE_NAME),
                    r -> assertEquals(defaultTriggerName, r.get("name"))
            );

            // install and show in foo db
            testCall(sysSession, "CALL apoc.trigger.install($dbName, $name, 'return 1', {})",
                    Map.of("dbName", FOO_DB, "name", fooTriggerName),
                    r -> assertEquals(fooTriggerName, r.get("name"))
            );

            testCall(sysSession, "CALL apoc.trigger.show($dbName)",
                    Map.of("dbName", FOO_DB),
                    r -> assertEquals(fooTriggerName, r.get("name"))
            );
        }
    }

    @Test
    public void testTriggerInstallInNewDatabase() {
        final String fooTriggerName = UUID.randomUUID().toString();

        try (Session sysSession = neo4jContainer.getDriver().session(forDatabase(SYSTEM_DATABASE_NAME))) {
            testCall(sysSession, "call apoc.trigger.install($dbName, $name, 'UNWIND $createdNodes AS n SET n.created = true', {})",
                    Map.of("dbName", FOO_DB, "name", fooTriggerName),
                    r -> assertEquals(fooTriggerName, r.get("name")));
        }

        final String queryTriggerList = "CALL apoc.trigger.list() YIELD name WHERE name = $name RETURN name";
        try (Session fooDbSession = neo4jContainer.getDriver().session(forDatabase(FOO_DB))) {
            assertEventually(() -> {
                final Result res = fooDbSession.run(queryTriggerList,
                        Map.of("name", fooTriggerName));
                assertTrue("Should have an element", res.hasNext());
                final Record next = res.next();
                assertEquals(fooTriggerName, next.get("name").asString());
                return !res.hasNext();
            }, value -> value, TIMEOUT, TimeUnit.SECONDS);

            fooDbSession.run("CREATE (:Something)");

            testCall(fooDbSession, "MATCH (n:Something) RETURN n.created AS created",
                    r -> assertEquals(true, r.get("created")));
        }

        // check that the trigger is correctly installed in 'foo' db only
        try (Session defaultDbSession = neo4jContainer.getDriver().session(forDatabase(DEFAULT_DATABASE_NAME))) {
            testResult(defaultDbSession, queryTriggerList,
                    Map.of("name", fooTriggerName),
                    r -> assertFalse(r.hasNext())
            );

            defaultDbSession.run("CREATE (:Something)");

            testCall(defaultDbSession, "MATCH (n:Something) RETURN n.created",
                    r -> assertNull(r.get("created")));
        }
    }

    @Test
    public void testTriggersAllowedOnlyWithAdmin() {

        try (Driver userDriver = GraphDatabase.driver(neo4jContainer.getBoltUrl(), AuthTokens.basic(NO_ADMIN_USER, NO_ADMIN_PWD))) {

            try (Session sysUserSession = userDriver.session(forDatabase(SYSTEM_DATABASE_NAME))) {
                failsWithNonAdminUser(sysUserSession, "apoc.trigger.install", "call apoc.trigger.install('neo4j', 'qwe', 'return 1', {})");
                failsWithNonAdminUser(sysUserSession, "apoc.trigger.drop", "call apoc.trigger.drop('neo4j', 'qwe')");
                failsWithNonAdminUser(sysUserSession, "apoc.trigger.dropAll", "call apoc.trigger.dropAll('neo4j')");
                failsWithNonAdminUser(sysUserSession, "apoc.trigger.stop", "call apoc.trigger.stop('neo4j', 'qwe')");
                failsWithNonAdminUser(sysUserSession, "apoc.trigger.start", "call apoc.trigger.start('neo4j', 'qwe')");
                failsWithNonAdminUser(sysUserSession, "apoc.trigger.show", "call apoc.trigger.show('neo4j')");
            }

            try (Session neo4jUserSession = userDriver.session(forDatabase(DEFAULT_DATABASE_NAME))) {
                failsWithNonAdminUser(neo4jUserSession, "apoc.trigger.add", "call apoc.trigger.add('abc', 'return 1', {})");
                failsWithNonAdminUser(neo4jUserSession, "apoc.trigger.remove", "call apoc.trigger.remove('abc')");
                failsWithNonAdminUser(neo4jUserSession, "apoc.trigger.removeAll", "call apoc.trigger.removeAll");
                failsWithNonAdminUser(neo4jUserSession, "apoc.trigger.pause", "call apoc.trigger.pause('abc')");
                failsWithNonAdminUser(neo4jUserSession, "apoc.trigger.resume", "call apoc.trigger.resume('abc')");
                failsWithNonAdminUser(neo4jUserSession, "apoc.trigger.list", "call apoc.trigger.list");
            }
        }
    }

    private void failsWithNonAdminUser(Session session, String procName, String query) {
        try {
            testCall(session, query,
                    row -> fail("Should fail because of non admin user") );
        } catch (Exception e) {
            String actual = e.getMessage();
            final String expected = String.format("Executing admin procedure '%s' permission has not been granted for user 'nonadmin'",
                    procName);
            assertTrue("Actual error message is: " + actual, actual.contains(expected));
        }
    }
}