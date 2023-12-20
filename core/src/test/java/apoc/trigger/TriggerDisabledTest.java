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

import static apoc.ApocConfig.APOC_TRIGGER_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static org.junit.Assert.assertThrows;

import apoc.util.TestUtil;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

/**
 * @author alexiudice
 * @since 14.07.18
 * <p>
 * Tests for fix of #845.
 * <p>
 * Testing disabled triggers needs to be a different test file from 'TriggerTest.java' since
 * Trigger classes and methods are static and 'TriggerTest.java' instantiates a class that enables triggers.
 *
 * NOTE: this test class expects every method to fail with a RuntimeException
 */
public class TriggerDisabledTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() {
        apocConfig().setProperty(APOC_TRIGGER_ENABLED, false);
        TestUtil.registerProcedure(db, Trigger.class);
    }

    @AfterClass
    public static void teardown() {
        db.shutdown();
    }

    @Test
    public void testTriggerDisabledList() {
        assertThrows(
                TriggerHandler.NOT_ENABLED_ERROR,
                RuntimeException.class,
                () -> db.executeTransactionally(
                        "CALL apoc.trigger.list() YIELD name RETURN name", Map.of(), Result::resultAsString));
    }

    @Test
    public void testTriggerDisabledAdd() {
        assertThrows(
                TriggerHandler.NOT_ENABLED_ERROR,
                RuntimeException.class,
                () -> db.executeTransactionally(
                        "CALL apoc.trigger.add('test-trigger', 'RETURN 1', {phase: 'before'}) YIELD name RETURN name"));
    }

    @Test
    public void testTriggerDisabledRemove() {
        assertThrows(
                TriggerHandler.NOT_ENABLED_ERROR,
                RuntimeException.class,
                () -> db.executeTransactionally("CALL apoc.trigger.remove('test-trigger')"));
    }

    @Test
    public void testTriggerDisabledResume() {
        assertThrows(
                TriggerHandler.NOT_ENABLED_ERROR,
                RuntimeException.class,
                () -> db.executeTransactionally("CALL apoc.trigger.resume('test-trigger')"));
    }

    @Test
    public void testTriggerDisabledPause() {
        assertThrows(
                TriggerHandler.NOT_ENABLED_ERROR,
                RuntimeException.class,
                () -> db.executeTransactionally("CALL apoc.trigger.pause('test-trigger')"));
    }
}
