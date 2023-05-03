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
package apoc.util.kernel;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static apoc.util.kernel.MultiThreadedGlobalGraphOperations.*;
import static apoc.util.kernel.MultiThreadedGlobalGraphOperations.GlobalOperationsTypes.NODES;
import static apoc.util.kernel.MultiThreadedGlobalGraphOperations.GlobalOperationsTypes.RELATIONSHIPS;
import static org.junit.Assert.assertEquals;

public class MultiThreadedGlobalGraphOperationsTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void beforeClass() {
        createData();
    }

    private static void createData() {
        db.executeTransactionally("UNWIND range(1,1000) as x MERGE (s{id:x}) MERGE (e{id:x+1}) merge (s)-[:REL{id:x}]->(e)");
    }

    @Test
    public void shouldforAllNodesWork() {
        AtomicInteger counter = new AtomicInteger();
        BatchJobResult result = forAllNodes(db, Executors.newFixedThreadPool(4), 10,
                (ktx,nodeCursor) -> counter.incrementAndGet());
        assertEquals(1001, counter.get());
        final long highestIdInUse = getHighestIdInUseForStore(db.getDependencyResolver(), NODES);
        assertEquals(Double.valueOf(Math.ceil(highestIdInUse / 10.0)).longValue() , result.getBatches());
        assertEquals( 1001, result.getSucceeded());

        long countOfNodes = TestUtil.singleResultFirstColumn(db, "match (n) return count(n) as count");

        assertEquals( 9, result.getMissing()); // TODO: why do we get 9 missings ?
        assertEquals( 0, result.getFailures());
    }

    @Test
    public void shouldforAllRelationshipsWork() {
        AtomicInteger counter = new AtomicInteger();
        BatchJobResult result = forAllRelationships(db, Executors.newFixedThreadPool(4), 10,
                (ktx, relationshipScanCursor) -> counter.incrementAndGet());
        assertEquals(1000, counter.get());
        final long highestIdInUse = getHighestIdInUseForStore(db.getDependencyResolver(), RELATIONSHIPS);
        assertEquals(Double.valueOf(Math.ceil(highestIdInUse / 10.0)).longValue(), result.getBatches());
        assertEquals( 1000, result.getSucceeded());
        assertEquals( 0, result.getMissing());
        assertEquals( 0, result.getFailures());
    }
}
