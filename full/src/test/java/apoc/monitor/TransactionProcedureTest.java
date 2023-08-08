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
package apoc.monitor;

import apoc.cypher.CypherInitializer;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.concurrent.atomic.AtomicLong;

import static apoc.util.CypherInitializerUtil.getInitializer;
import static apoc.util.TestUtil.testCall;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;

public class TransactionProcedureTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setup() {
        TestUtil.registerProcedure(db, Transaction.class);
        // we need to wait until every CypherInitializer transaction is finished to make sure tests are not flaky
        waitForInitializerBeingFinished(db);
    }

    @After
    public void teardown() {
        db.shutdown();
    }

    @Test
    public void testGetStoreInfo() {
        AtomicLong peakTx = new AtomicLong();
        AtomicLong lastTxId = new AtomicLong();
        AtomicLong totalOpenedTx = new AtomicLong();
        AtomicLong totalTx = new AtomicLong();
        testCall(db, "CALL apoc.monitor.tx()", (row) -> {
            assertEquals(0l, row.get("rolledBackTx"));
            peakTx.set((long) row.get("peakTx"));
            assertThat(peakTx.get(), isOneOf(1l, 2l));
            assertEquals(3l, lastTxId.addAndGet((long) row.get("lastTxId")));
            assertEquals(1l, row.get("currentOpenedTx"));
            assertEquals(4l, totalOpenedTx.addAndGet((long) row.get("totalOpenedTx")));
            assertEquals(3l, totalTx.addAndGet((long )row.get("totalTx")));
        });

        db.executeTransactionally("create ()");
        testCall(db, "CALL apoc.monitor.tx()", (row) -> {
            assertEquals(0l, row.get("rolledBackTx"));
            assertEquals(peakTx.get(), row.get("peakTx"));
            assertEquals(lastTxId.incrementAndGet(), row.get("lastTxId"));
            assertEquals(1l, row.get("currentOpenedTx"));
            assertEquals(totalOpenedTx.addAndGet(2L), row.get("totalOpenedTx"));
            assertEquals(totalTx.addAndGet(2L), row.get("totalTx"));
        });
    }

    // equivalent to CypherInitializerTest.waitForInitializerBeingFinished
    private static void waitForInitializerBeingFinished(DbmsRule dbmsRule) {
        CypherInitializer initializer = getInitializer(dbmsRule.databaseName(), dbmsRule, CypherInitializer.class);
        while (!initializer.isFinished()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }
}
