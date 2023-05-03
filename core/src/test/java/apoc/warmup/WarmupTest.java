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
package apoc.warmup;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static org.junit.Assert.assertEquals;

/**
 * @author Sascha Peukert
 * @since 06.05.16
 */
public class WarmupTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, Warmup.class);
        // Create enough nodes and relationships to span 2 pages
        db.executeTransactionally("CREATE CONSTRAINT ON (f:Foo) ASSERT f.foo IS UNIQUE");
        db.executeTransactionally("UNWIND range(1, 300) AS i CREATE (n:Foo {foo:i})-[:KNOWS {bar:2}]->(m {foobar:3, array:range(1,100)})");
        // Delete all relationships and their nodes, but ones with the minimum and maximum relationship ids, so
        // they still span 2 pages
        db.executeTransactionally("MATCH ()-[r:KNOWS]->() " +
                "WITH [min(id(r)), max(id(r))] AS ids " +
                "MATCH (n)-[r:KNOWS]->(m) " +
                "WHERE NOT id(r) IN ids " +
                "DELETE n, m, r");
    }

    @Test
    public void testWarmup() throws Exception {
        TestUtil.testCall(db, "CALL apoc.warmup.run()", r -> {
            assertEquals(4L, r.get("nodesTotal"));
            assertEquals(2L, r.get("nodePages"));
            assertEquals(2L, r.get("relsTotal"));
            assertEquals(2L, r.get("relPages"));
        });
    }

    @Test
    public void testWarmupProperties() throws Exception {
        TestUtil.testCall(db, "CALL apoc.warmup.run(true)", r -> {
            assertEquals(true, r.get("propertiesLoaded"));
            assertEquals(5L, r.get("propPages"));
        });
    }

    @Test
    public void testWarmupDynamicProperties() throws Exception {
        TestUtil.testCall(db, "CALL apoc.warmup.run(true,true)", r -> {
            assertEquals(true, r.get("propertiesLoaded"));
            assertEquals(true, r.get("dynamicPropertiesLoaded"));
            assertEquals(5L, r.get("arrayPropPages"));
        });
    }

    @Test
    public void testWarmupIndexes() throws Exception {
        TestUtil.testCall(db, "CALL apoc.warmup.run(true,true,true)", r -> {
            assertEquals(true, r.get("indexesLoaded"));
            assertEquals(6L, r.get("indexPages"));
        });
    }
}
