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

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

public class IdsProcedureTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setup() {
        TestUtil.registerProcedure(db, Ids.class);
    }

    @Test
    public void testGetNodeIdsInUse() {
        createData();
        testCall(db, "CALL apoc.monitor.ids()", (row) -> {
            long nodeIds = (long) row.get("nodeIds");
            long relIds = (long) row.get("relIds");
            long propIds = (long) row.get("propIds");
            long relTypeIds = (long) row.get("relTypeIds");
            assertEquals(true, nodeIds >= 6L);
            // no longer correct, due to batch id-allocation:
            // assertEquals(6L, nodeIds);
            // assertEquals(2L, relIds);
            // assertEquals(1L, propIds);
            assertEquals(true, relIds >= 2L);
            assertEquals(true, propIds >= 1L);
            assertEquals(2L, relTypeIds);
        });
    }

    private void createData() {
        try (Transaction tx = db.beginTx()) {
            tx.execute("CREATE (n)");
            tx.execute("CREATE (n)-[:REL_TYPE1]->(n2)");
            tx.execute("CREATE (n)-[:REL_TYPE2]->(n2)");
            tx.execute("CREATE (n) SET n.key = 123");
            tx.commit();
        }
    }

}
