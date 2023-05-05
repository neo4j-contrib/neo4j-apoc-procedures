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
package apoc.get;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Collections;
import java.util.List;

import static apoc.util.MapUtil.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author mh
 * @since 16.04.16
 */
public class GetProceduresTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, GetProcedures.class);
    }

    @Test
    public void testNodes() throws Exception {
        List<Long> ids = TestUtil.firstColumn(db, "UNWIND range(0,2) as id CREATE (n:Node {id:id}) return id(n) as id");
        TestUtil.testResult(db, "CALL apoc.get.nodes($ids)", map("ids",ids), r -> {
            assertEquals(true, ids.contains(((Node) r.next().get("node")).getId()));
            assertEquals(true, ids.contains(((Node) r.next().get("node")).getId()));
            assertEquals(true, ids.contains(((Node) r.next().get("node")).getId()));
        });
    }

    @Test
    public void testRels() throws Exception {
        List<Long> ids = TestUtil.firstColumn(db, "CREATE (n) WITH n UNWIND range(0,2) as id CREATE (n)-[r:KNOWS]->(n) return id(r) as id");
        TestUtil.testResult(db, "CALL apoc.get.rels($ids)", map("ids",ids), r -> {
            assertEquals(true, ids.contains(((Relationship) r.next().get("rel")).getId()));
            assertEquals(true, ids.contains(((Relationship) r.next().get("rel")).getId()));
            assertEquals(true, ids.contains(((Relationship) r.next().get("rel")).getId()));
        });
    }

    @Test
    public void testArrayOfIds() {
        String query = "MERGE (g:Foo {id: 1})\n" +
                "MERGE (h:Foo {id: 2})\n" +
                "MERGE (g)-[r:BAR]->(h)\n" +
                "MERGE (t:Temp)\n" +
                "SET t.ids = [] + ID(r)\n" +
                "WITH t\n" +
                "CALL apoc.get.rels(t.ids) YIELD rel\n" +
                "RETURN rel";
        TestUtil.testResult(db, query, Collections.emptyMap(), r -> {
            final ResourceIterator<Relationship> relIT = r.columnAs("rel");
            final Relationship rel = relIT.next();
            assertEquals("BAR", rel.getType().name());
            assertFalse(r.hasNext());
        });
    }
}
