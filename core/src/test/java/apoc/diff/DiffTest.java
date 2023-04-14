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
package apoc.diff;

import apoc.create.Create;
import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author Benjamin Clauss
 * @since 15.06.2018
 */
public class DiffTest {

    private static Node node1;
    private static Node node2;
    private static Node node3;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setup() throws Exception {
        TestUtil.registerProcedure(db, Diff.class, Create.class);

        try (Transaction tx = db.beginTx()) {
            node1 = tx.createNode(Label.label("Node1"));
            node1.setProperty("prop1", "val1");
            node1.setProperty("prop2", 2L);

            node2 = tx.createNode(Label.label("Node2"));
            node2.setProperty("prop1", "val1");
            node2.setProperty("prop2", 2L);
            node2.setProperty("prop4", "four");

            node3 = tx.createNode(Label.label("Node3"));
            node3.setProperty("prop1", "val1");
            node3.setProperty("prop3", "3");
            node3.setProperty("prop4", "for");
            tx.commit();
        }
    }

    @Test
    public void nodesWithList() {
        final List<String> list = List.of("tomatoes", "bread", "cookies");
        TestUtil.testCall(db, "CREATE (charlie:Person $propCharlie), (hannah:Person $propHanna)\n" +
                "RETURN apoc.diff.nodes(charlie, hannah) AS result", 
                Map.of("propCharlie", Map.of("name", "Charlie", "alpha", "one", "born", 1999, "grocery_list", list),
                        "propHanna", Map.of("name", "Hannah", "beta", "two", "born", 1999, "grocery_list", list)),
                r -> {
            Map<String, Map<String, Object>> res = (Map<String, Map<String, Object>>) r.get("result");
            Map<String, Object> inCommon = res.get("inCommon");
            assertArrayEquals(list.toArray(), (String[]) inCommon.get("grocery_list"));
            assertEquals(1999, inCommon.get("born"));
            assertEquals(Map.of("alpha", "one"), res.get("leftOnly"));
            assertEquals(Map.of("beta", "two"), res.get("rightOnly"));
            assertEquals(Map.of("name", Map.of("left", "Charlie", "right", "Hannah")), res.get("different"));
        });
    }

    @Test
    public void nodesSame() {
        Map<String, Object> result =
                db.executeTransactionally(
                        "MATCH (node:Node1) RETURN apoc.diff.nodes(node, node) as diff", new HashMap<>(),
                        r -> Iterators.single(r.columnAs("diff")));
        assertNotNull(result);

        HashMap<String, Object> leftOnly = (HashMap<String, Object>) result.get("leftOnly");
        assertTrue(leftOnly.isEmpty());

        HashMap<String, Object> rightOnly = (HashMap<String, Object>) result.get("rightOnly");
        assertTrue(rightOnly.isEmpty());

        HashMap<String, Object> different = (HashMap<String, Object>) result.get("different");
        assertTrue(different.isEmpty());

        HashMap<String, Object> inCommon = (HashMap<String, Object>) result.get("inCommon");
        assertEquals(2, inCommon.size());
        assertEquals("val1", inCommon.get("prop1"));
        assertEquals(2L, inCommon.get("prop2"));
    }

    @Test
    public void nodesDiffering() {
        Map<String, Object> result =
                db.executeTransactionally(
                        "MATCH (leftNode:Node2), (rightNode:Node3) RETURN apoc.diff.nodes(leftNode, rightNode) as diff", new HashMap<>(),
                        r -> Iterators.single(r.columnAs("diff")));
        assertNotNull(result);

        HashMap<String, Object> leftOnly = (HashMap<String, Object>) result.get("leftOnly");
        assertEquals(1, leftOnly.size());
        assertEquals(2L, leftOnly.get("prop2"));

        HashMap<String, Object> rightOnly = (HashMap<String, Object>) result.get("rightOnly");
        assertEquals(1, rightOnly.size());
        assertEquals("3", rightOnly.get("prop3"));

        HashMap<String, HashMap<String, Object>> different = (HashMap<String, HashMap<String, Object>>) result.get("different");
        assertEquals(1, different.size());
        HashMap<String, Object> pairs = different.get("prop4");
        assertEquals("four", pairs.get("left"));
        assertEquals("for", pairs.get("right"));

        HashMap<String, Object> inCommon = (HashMap<String, Object>) result.get("inCommon");
        assertEquals(1, inCommon.size());
        assertEquals("val1", inCommon.get("prop1"));
    }

    @Test
    public void shouldBeDiffWithVirtualNodes() {
        String query = "WITH apoc.create.vNode(['Node2'], {prop1: 'val1', prop2: 2, prop4: 'four'}) AS nodeA, " +
                "apoc.create.vNode(['Node3'], {prop1: 'val1', prop3: '3', prop4: 'for'}) AS nodeB " +
                "RETURN apoc.diff.nodes(nodeA, nodeB) as diff";
        Map<String, Object> result =
                db.executeTransactionally(query, Map.of(),
                        r -> Iterators.single(r.columnAs("diff")));
        assertNotNull(result);

        HashMap<String, Object> leftOnly = (HashMap<String, Object>) result.get("leftOnly");
        assertEquals(1, leftOnly.size());
        assertEquals(2L, leftOnly.get("prop2"));

        HashMap<String, Object> rightOnly = (HashMap<String, Object>) result.get("rightOnly");
        assertEquals(1, rightOnly.size());
        assertEquals("3", rightOnly.get("prop3"));

        HashMap<String, HashMap<String, Object>> different = (HashMap<String, HashMap<String, Object>>) result.get("different");
        assertEquals(1, different.size());
        HashMap<String, Object> pairs = different.get("prop4");
        assertEquals("four", pairs.get("left"));
        assertEquals("for", pairs.get("right"));

        HashMap<String, Object> inCommon = (HashMap<String, Object>) result.get("inCommon");
        assertEquals(1, inCommon.size());
        assertEquals("val1", inCommon.get("prop1"));
    }

    @Test
    public void shouldBeSameWithVirtualNodes() {
        String query = "WITH apoc.create.vNode(['Node1'], {prop1: 'val1', prop2: 2}) AS node " +
                "RETURN apoc.diff.nodes(node, node) as diff";
        Map<String, Object> result =
                db.executeTransactionally(query, Map.of(),
                        r -> Iterators.single(r.columnAs("diff")));
        assertNotNull(result);

        HashMap<String, Object> leftOnly = (HashMap<String, Object>) result.get("leftOnly");
        assertTrue(leftOnly.isEmpty());

        HashMap<String, Object> rightOnly = (HashMap<String, Object>) result.get("rightOnly");
        assertTrue(rightOnly.isEmpty());

        HashMap<String, Object> different = (HashMap<String, Object>) result.get("different");
        assertTrue(different.isEmpty());

        HashMap<String, Object> inCommon = (HashMap<String, Object>) result.get("inCommon");
        assertEquals(2, inCommon.size());
        assertEquals("val1", inCommon.get("prop1"));
        assertEquals(2L, inCommon.get("prop2"));
    }

}
