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
package apoc.generate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.internal.helpers.collection.Iterables.firstOrNull;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

/**
 * Integration test for {@link Generate}.
 */
public class GenerateTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, Generate.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void shouldGenerateErdosRenyi1() {
        db.executeTransactionally("CALL apoc.generate.er(20,100,null,null)");

        try (Transaction tx = db.beginTx()) {
            assertEquals(20, count(tx.getAllNodes()));
            assertEquals(100, count(tx.getAllRelationships()));
            assertEquals(
                    "Person",
                    firstOrNull(firstOrNull(tx.getAllNodes()).getLabels()).name());
            assertEquals(
                    "FRIEND_OF", firstOrNull(tx.getAllRelationships()).getType().name());
            assertTrue(firstOrNull(tx.getAllNodes()).hasProperty("name"));

            tx.commit();
        }
    }

    @Test
    public void shouldGenerateErdosRenyi2() {
        db.executeTransactionally("CALL apoc.generate.er(null,null,null,null)");

        try (Transaction tx = db.beginTx()) {
            assertEquals(1000, count(tx.getAllNodes()));
            assertEquals(10000, count(tx.getAllRelationships()));
            assertEquals(
                    "Person",
                    firstOrNull(firstOrNull(tx.getAllNodes()).getLabels()).name());
            assertEquals(
                    "FRIEND_OF", firstOrNull(tx.getAllRelationships()).getType().name());
            assertTrue(firstOrNull(tx.getAllNodes()).hasProperty("name"));

            tx.commit();
        }
    }

    @Test
    public void shouldGenerateErdosRenyi3() {
        db.executeTransactionally("CALL apoc.generate.er(20,100,'TestLabel','TEST_REL')");

        try (Transaction tx = db.beginTx()) {
            assertEquals(20, count(tx.getAllNodes()));
            assertEquals(100, count(tx.getAllRelationships()));
            assertEquals(
                    "TestLabel",
                    firstOrNull(firstOrNull(tx.getAllNodes()).getLabels()).name());
            assertEquals(
                    "TEST_REL", firstOrNull(tx.getAllRelationships()).getType().name());
            assertTrue(firstOrNull(tx.getAllNodes()).hasProperty("uuid"));

            tx.commit();
        }
    }

    @Test
    public void shouldGenerateBarabasiAlbert1() {
        db.executeTransactionally("CALL apoc.generate.ba(100,3,null,null)");

        try (Transaction tx = db.beginTx()) {
            assertEquals(100, count(tx.getAllNodes()));
            assertEquals(294, count(tx.getAllRelationships()));
            assertEquals(
                    "Person",
                    firstOrNull(firstOrNull(tx.getAllNodes()).getLabels()).name());
            assertEquals(
                    "FRIEND_OF", firstOrNull(tx.getAllRelationships()).getType().name());
            assertTrue(firstOrNull(tx.getAllNodes()).hasProperty("name"));
            tx.commit();
        }
    }

    @Test
    public void shouldGenerateBarabasiAlbert2() {
        db.executeTransactionally("CALL apoc.generate.ba(100,3,'TestLabel','TEST_REL')");

        try (Transaction tx = db.beginTx()) {
            assertEquals(100, count(tx.getAllNodes()));
            assertEquals(294, count(tx.getAllRelationships()));
            assertEquals(
                    "TestLabel",
                    firstOrNull(firstOrNull(tx.getAllNodes()).getLabels()).name());
            assertEquals(
                    "TEST_REL", firstOrNull(tx.getAllRelationships()).getType().name());
            assertTrue(firstOrNull(tx.getAllNodes()).hasProperty("uuid"));
            tx.commit();
        }
    }

    @Test
    public void shouldGenerateBarabasiAlbert3() {
        db.executeTransactionally("CALL apoc.generate.ba(null,null,'TestLabel','TEST_REL')");

        try (Transaction tx = db.beginTx()) {
            assertEquals(1000, count(tx.getAllNodes()));
            assertEquals(1997, count(tx.getAllRelationships()));
            assertEquals(
                    "TestLabel",
                    firstOrNull(firstOrNull(tx.getAllNodes()).getLabels()).name());
            assertEquals(
                    "TEST_REL", firstOrNull(tx.getAllRelationships()).getType().name());
            assertTrue(firstOrNull(tx.getAllNodes()).hasProperty("uuid"));
            tx.commit();
        }
    }

    @Test
    public void shouldGenerateWattsStrogatz1() {
        db.executeTransactionally("CALL apoc.generate.ws(100,10,0.5,null,null)");

        try (Transaction tx = db.beginTx()) {
            assertEquals(100, count(tx.getAllNodes()));
            assertEquals(500, count(tx.getAllRelationships()));
            assertEquals(
                    "Person",
                    firstOrNull(firstOrNull(tx.getAllNodes()).getLabels()).name());
            assertEquals(
                    "FRIEND_OF", firstOrNull(tx.getAllRelationships()).getType().name());
            assertTrue(firstOrNull(tx.getAllNodes()).hasProperty("name"));
            tx.commit();
        }
    }

    @Test
    public void shouldGenerateWattsStrogatz2() {
        db.executeTransactionally("CALL apoc.generate.ws(null,null,null,null,null)");

        try (Transaction tx = db.beginTx()) {
            assertEquals(1000, count(tx.getAllNodes()));
            assertEquals(2000, count(tx.getAllRelationships()));
            assertEquals(
                    "Person",
                    firstOrNull(firstOrNull(tx.getAllNodes()).getLabels()).name());
            assertEquals(
                    "FRIEND_OF", firstOrNull(tx.getAllRelationships()).getType().name());
            assertTrue(firstOrNull(tx.getAllNodes()).hasProperty("name"));
            tx.commit();
        }
    }

    @Test
    public void shouldGenerateWattsStrogatz3() {
        db.executeTransactionally("CALL apoc.generate.ws(null,null,null,'TestLabel','TEST_REL')");

        try (Transaction tx = db.beginTx()) {
            assertEquals(1000, count(tx.getAllNodes()));
            assertEquals(2000, count(tx.getAllRelationships()));
            assertEquals(
                    "TestLabel",
                    firstOrNull(firstOrNull(tx.getAllNodes()).getLabels()).name());
            assertEquals(
                    "TEST_REL", firstOrNull(tx.getAllRelationships()).getType().name());
            assertTrue(firstOrNull(tx.getAllNodes()).hasProperty("uuid"));
            tx.commit();
        }
    }

    @Test
    public void shouldGenerateComplete1() {
        db.executeTransactionally("CALL apoc.generate.complete(10,'TestLabel','TEST_REL')");

        try (Transaction tx = db.beginTx()) {
            assertEquals(10, count(tx.getAllNodes()));
            assertEquals(45, count(tx.getAllRelationships()));
            assertEquals(
                    "TestLabel",
                    firstOrNull(firstOrNull(tx.getAllNodes()).getLabels()).name());
            assertEquals(
                    "TEST_REL", firstOrNull(tx.getAllRelationships()).getType().name());
            assertTrue(firstOrNull(tx.getAllNodes()).hasProperty("uuid"));
            tx.commit();
        }
    }

    @Test
    public void shouldGenerateComplete2() {
        db.executeTransactionally("CALL apoc.generate.complete(10,null,null)");

        try (Transaction tx = db.beginTx()) {
            assertEquals(10, count(tx.getAllNodes()));
            assertEquals(45, count(tx.getAllRelationships()));
            assertEquals(
                    "Person",
                    firstOrNull(firstOrNull(tx.getAllNodes()).getLabels()).name());
            assertEquals(
                    "FRIEND_OF", firstOrNull(tx.getAllRelationships()).getType().name());
            assertTrue(firstOrNull(tx.getAllNodes()).hasProperty("name"));
            tx.commit();
        }
    }

    @Test
    public void shouldGenerateSimple1() {
        db.executeTransactionally("CALL apoc.generate.simple([2,2,2,2],'TestLabel','TEST_REL')");

        try (Transaction tx = db.beginTx()) {
            assertEquals(4, count(tx.getAllNodes()));
            assertEquals(4, count(tx.getAllRelationships()));
            assertEquals(
                    "TestLabel",
                    firstOrNull(firstOrNull(tx.getAllNodes()).getLabels()).name());
            assertEquals(
                    "TEST_REL", firstOrNull(tx.getAllRelationships()).getType().name());
            assertTrue(firstOrNull(tx.getAllNodes()).hasProperty("uuid"));
            tx.commit();
        }
    }

    @Test
    public void shouldGenerateSimple2() {
        db.executeTransactionally("CALL apoc.generate.simple([2,2,2,2],null,null)");

        try (Transaction tx = db.beginTx()) {
            assertEquals(4, count(tx.getAllNodes()));
            assertEquals(4, count(tx.getAllRelationships()));
            assertEquals(
                    "Person",
                    firstOrNull(firstOrNull(tx.getAllNodes()).getLabels()).name());
            assertEquals(
                    "FRIEND_OF", firstOrNull(tx.getAllRelationships()).getType().name());
            assertTrue(firstOrNull(tx.getAllNodes()).hasProperty("name"));
            tx.commit();
        }
    }
}
