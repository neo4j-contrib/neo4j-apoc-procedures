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
package apoc.convert;

import static org.junit.Assert.assertEquals;

import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

public class PathsToJsonTreeTest {
    private Object parseJson(String json) {
        return JsonUtil.parse(json, null, null);
    }

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, Json.class);
    }

    @After
    public void teardown() {
        db.shutdown();
    }

    @After
    public void clear() {
        db.executeTransactionally("MATCH (n) DETACH DELETE n;");
    }

    @Test
    public void testToTreeSimplePath() throws Exception {
        /*            r:R
              a:A --------> b:B
        */
        db.executeTransactionally("CREATE (a: A {nodeName: 'a'})-[r: R {relName: 'r'}]->(b: B {nodeName: 'b'})");

        var query = "MATCH path = (n)-[r]->(m)\n"
                + "WITH COLLECT(path) AS paths\n"
                + "CALL apoc.paths.toJsonTree(paths, true, {sortPaths: false}) YIELD value AS tree\n"
                + "RETURN tree";

        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(query);
            var rows = result.stream().collect(Collectors.toList());
            var expectedRow = "{" + "   \"tree\":{"
                    + "      \"nodeName\":\"a\","
                    + "      \"r\":["
                    + "         {"
                    + "            \"nodeName\":\"b\","
                    + "            \"r._id\":0,"
                    + "            \"_type\":\"B\","
                    + "            \"_id\":1,"
                    + "            \"r.relName\":\"r\""
                    + "         }"
                    + "      ],"
                    + "      \"_type\":\"A\","
                    + "      \"_id\":0"
                    + "   }"
                    + "}";
            assertEquals(rows.size(), 1);
            assertEquals(parseJson(expectedRow), rows.get(0));
        }
    }

    @Test
    public void testToTreeSimpleReversePath() {
        /*            r:R
              a:A <-------- b:B
        */
        db.executeTransactionally("CREATE " + "(a: A {nodeName: 'a'})<-[r: R {relName: 'r'}]-(b: B {nodeName: 'b'})");

        var query = "MATCH path = (n)<-[r]-(m)\n"
                + "WITH COLLECT(path) AS paths\n"
                + "CALL apoc.paths.toJsonTree(paths, true, {sortPaths: false}) YIELD value AS tree\n"
                + "RETURN tree";

        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(query);
            var rows = result.stream().collect(Collectors.toList());
            var expectedRow = "{" + "   \"tree\":{"
                    + "      \"nodeName\":\"b\","
                    + "      \"r\":["
                    + "         {"
                    + "            \"nodeName\":\"a\","
                    + "            \"r._id\":0,"
                    + "            \"_type\":\"A\","
                    + "            \"_id\":0,"
                    + "            \"r.relName\":\"r\""
                    + "         }"
                    + "      ],"
                    + "      \"_type\":\"B\","
                    + "      \"_id\":1"
                    + "   }"
                    + "}";
            assertEquals(rows.size(), 1);
            assertEquals(parseJson(expectedRow), rows.get(0));
        }
    }

    @Test
    public void testToTreeSimpleBidirectionalPath() {
        /*         r1:R
                 -------->
             a:A          b:B
                 <--------
                   r2:R
        */
        db.executeTransactionally("CREATE "
                + "(a: A {nodeName: 'a'})<-[r1: R {relName: 'r'}]-(b: B {nodeName: 'b'}),"
                + "(a)-[r2: R {relName: 'r'}]->(b)");

        var query = "MATCH path = (n)-[r]-(m)\n"
                + "WITH COLLECT(path) AS paths\n"
                + "CALL apoc.paths.toJsonTree(paths, true, {sortPaths: false}) YIELD value AS tree\n"
                + "RETURN tree";

        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(query);
            var rows = result.stream().collect(Collectors.toList());
            var expectedRow = "{" + "   \"tree\":{"
                    + "      \"nodeName\":\"a\","
                    + "      \"r\":["
                    + "         {"
                    + "            \"nodeName\":\"b\","
                    + "            \"r._id\":1,"
                    + "            \"r\":["
                    + "               {"
                    + "                  \"nodeName\":\"a\","
                    + "                  \"r._id\":0,"
                    + "                  \"_type\":\"A\","
                    + "                  \"_id\":0,"
                    + "                  \"r.relName\":\"r\""
                    + "               }"
                    + "            ],"
                    + "            \"_type\":\"B\","
                    + "            \"_id\":1,"
                    + "            \"r.relName\":\"r\""
                    + "         }"
                    + "      ],"
                    + "      \"_type\":\"A\","
                    + "      \"_id\":0"
                    + "   }"
                    + "}";
            assertEquals(rows.size(), 1);
            assertEquals(parseJson(expectedRow), rows.get(0));
        }
    }

    @Test
    public void testToTreeSimpleBidirectionalQuery() {
        /*         r1:R
             a:A --------> b:B
        */
        db.executeTransactionally("CREATE (a: A {nodeName: 'a'})-[r1: R {relName: 'r'}]->(b: B {nodeName: 'b'})");

        // Note this would be returning both the path (a)-[r]->(b) and (b)<-[r]-(a)
        // but we only expect a tree starting in 'a'
        var query = "MATCH path = (n)-[r]-(m)\n"
                + "WITH COLLECT(path) AS paths\n"
                + "CALL apoc.paths.toJsonTree(paths, true, {sortPaths: false}) YIELD value AS tree\n"
                + "RETURN tree";

        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(query);
            var rows = result.stream().collect(Collectors.toList());
            var expectedRow = "{" + "   \"tree\":{"
                    + "      \"nodeName\":\"a\","
                    + "      \"r\":["
                    + "         {"
                    + "            \"nodeName\":\"b\","
                    + "            \"r._id\":0,"
                    + "            \"_type\":\"B\","
                    + "            \"_id\":1,"
                    + "            \"r.relName\":\"r\""
                    + "         }"
                    + "      ],"
                    + "      \"_type\":\"A\","
                    + "      \"_id\":0"
                    + "   }"
                    + "}";
            assertEquals(rows.size(), 1);
            assertEquals(parseJson(expectedRow), rows.get(0));
        }
    }

    @Test
    public void testToTreeBidirectionalPathAndQuery() {
        /*          r1:R1         r2:R2
              a:A ---------> b:B --------> a
        */
        db.executeTransactionally(
                "CREATE (a: A {nodeName: 'a'})-[r1: R1 {relName: 'r1'}]->(b: B {nodeName: 'b'})-[r2: R2 {relName: 'r2'}]->(a)");

        // The query is bidirectional in this case, so
        // we would have duplicated paths, but we do not
        // expect duplicated trees
        var query = "MATCH path = (n)-[r]-(m)\n"
                + "WITH COLLECT(path) AS paths\n"
                + "CALL apoc.paths.toJsonTree(paths, true, {sortPaths: false}) YIELD value AS tree\n"
                + "RETURN tree";

        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(query);
            var rows = result.stream().collect(Collectors.toList());

            assertEquals(rows.size(), 1);
            var expectedRow = "{" + "   \"tree\":{"
                    + "      \"nodeName\":\"b\","
                    + "      \"_type\":\"B\","
                    + "      \"_id\":1,"
                    + "      \"r2\":["
                    + "         {"
                    + "            \"nodeName\":\"a\","
                    + "            \"r1\":["
                    + "               {"
                    + "                  \"nodeName\":\"b\","
                    + "                  \"r1._id\":0,"
                    + "                  \"_type\":\"B\","
                    + "                  \"r1.relName\":\"r1\","
                    + "                  \"_id\":1"
                    + "               }"
                    + "            ],"
                    + "            \"_type\":\"A\","
                    + "            \"r2._id\":1,"
                    + "            \"_id\":0,"
                    + "            \"r2.relName\":\"r2\""
                    + "         }"
                    + "      ]"
                    + "   }"
                    + "}";
            assertEquals(parseJson(expectedRow), rows.get(0));
        }
    }

    @Test
    public void testToTreeComplexGraph() {
        /*          r1:R1         r2:R2
              a:A --------> b:B ------> c:C
                             |
                      r3:R3  |
                            \|/
                            d:D
        */
        db.executeTransactionally("CREATE " + "(a: A {nodeName: 'a'})-[r1: R1 {relName: 'r1'}]->(b: B {nodeName: 'b'}),"
                + "(b)-[r2: R2 {relName: 'r2'}]->(c: C {nodeName: 'c'}),"
                + "(b)-[r3: R3 {relName: 'r3'}]->(d: D {nodeName: 'd'})");

        var query = "MATCH path = (n)-[r]->(m)\n"
                + "WITH COLLECT(path) AS paths\n"
                + "CALL apoc.paths.toJsonTree(paths, true, {sortPaths: false}) YIELD value AS tree\n"
                + "RETURN tree";

        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(query);
            var rows = result.stream().collect(Collectors.toList());

            assertEquals(rows.size(), 1);
            var expectedRow = "{" + "  \"tree\": {"
                    + "    \"nodeName\": \"a\","
                    + "    \"_type\": \"A\","
                    + "    \"_id\": 0,"
                    + "    \"r1\": ["
                    + "      {"
                    + "        \"nodeName\": \"b\","
                    + "        \"r2\": ["
                    + "          {"
                    + "            \"nodeName\": \"c\","
                    + "            \"r2._id\": 1,"
                    + "            \"_type\": \"C\","
                    + "            \"r2.relName\": \"r2\","
                    + "            \"_id\": 2"
                    + "          }"
                    + "        ],"
                    + "        \"r3\": ["
                    + "          {"
                    + "            \"nodeName\": \"d\","
                    + "            \"r3._id\": 2,"
                    + "            \"r3.relName\": \"r3\","
                    + "            \"_type\": \"D\","
                    + "            \"_id\": 3"
                    + "          }"
                    + "        ],"
                    + "        \"_type\": \"B\","
                    + "        \"r1._id\": 0,"
                    + "        \"_id\": 1,"
                    + "        \"r1.relName\": \"r1\""
                    + "      }"
                    + "    ]"
                    + "  }"
                    + "}";
            assertEquals(parseJson(expectedRow), rows.get(0));
        }
    }

    @Test
    public void testToTreeComplexGraphBidirectionalQuery() {
        /*          r1:R1         r2:R2
              a:A --------> b:B -------> c:C
                             |
                      r3:R3  |
                            \|/
                            d:D
        */
        db.executeTransactionally("CREATE " + "(a: A {nodeName: 'a'})-[r1: R1 {relName: 'r1'}]->(b: B {nodeName: 'b'}),"
                + "(b)-[r2: R2 {relName: 'r2'}]->(c: C {nodeName: 'c'}),"
                + "(b)-[r3: R3 {relName: 'r3'}]->(d: D {nodeName: 'd'})");

        // The query is bidirectional in this case, we don't expect duplicated paths
        var query = "MATCH path = (n)-[r]-(m)\n"
                + "WITH COLLECT(path) AS paths\n"
                + "CALL apoc.paths.toJsonTree(paths, true, {sortPaths: false}) YIELD value AS tree\n"
                + "RETURN tree";

        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(query);
            var rows = result.stream().collect(Collectors.toList());

            assertEquals(rows.size(), 1);
            var expectedRow = "{" + "  \"tree\": {"
                    + "    \"nodeName\": \"a\","
                    + "    \"_type\": \"A\","
                    + "    \"_id\": 0,"
                    + "    \"r1\": ["
                    + "      {"
                    + "        \"nodeName\": \"b\","
                    + "        \"r2\": ["
                    + "          {"
                    + "            \"nodeName\": \"c\","
                    + "            \"r2._id\": 1,"
                    + "            \"_type\": \"C\","
                    + "            \"r2.relName\": \"r2\","
                    + "            \"_id\": 2"
                    + "          }"
                    + "        ],"
                    + "        \"r3\": ["
                    + "          {"
                    + "            \"nodeName\": \"d\","
                    + "            \"r3._id\": 2,"
                    + "            \"r3.relName\": \"r3\","
                    + "            \"_type\": \"D\","
                    + "            \"_id\": 3"
                    + "          }"
                    + "        ],"
                    + "        \"_type\": \"B\","
                    + "        \"r1._id\": 0,"
                    + "        \"_id\": 1,"
                    + "        \"r1.relName\": \"r1\""
                    + "      }"
                    + "    ]"
                    + "  }"
                    + "}";
            assertEquals(parseJson(expectedRow), rows.get(0));
        }
    }

    @Test
    public void testToTreeGraphWithLoops() {
        /*          r1:R1          r2:R2
              a:A ---------> b:B --------> c:C
                            /  /|\
                            |___|
                            r3:R3
        */
        db.executeTransactionally("CREATE " + "(a: A {nodeName: 'a'})-[r1: R1 {relName: 'r1'}]->(b: B {nodeName: 'b'}),"
                + "(b)-[r2: R2 {relName: 'r2'}]->(c:C {nodeName: 'c'}),"
                + "(b)-[r3: R3 {relName: 'r3'}]->(b)");

        var query = "MATCH path = (n)-[r]->(m)\n"
                + "WITH COLLECT(path) AS paths\n"
                + "CALL apoc.paths.toJsonTree(paths, true, {sortPaths: false}) YIELD value AS tree\n"
                + "RETURN tree";

        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(query);
            var rows = result.stream().collect(Collectors.toList());

            assertEquals(rows.size(), 1);
            var expectedRow = "{" + "   \"tree\":{"
                    + "      \"nodeName\":\"a\","
                    + "      \"_type\":\"A\","
                    + "      \"_id\":0,"
                    + "      \"r1\":["
                    + "         {"
                    + "            \"nodeName\":\"b\","
                    + "            \"r2\":["
                    + "               {"
                    + "                  \"nodeName\":\"c\","
                    + "                  \"r2._id\":1,"
                    + "                  \"_type\":\"C\","
                    + "                  \"r2.relName\":\"r2\","
                    + "                  \"_id\":2"
                    + "               }"
                    + "            ],"
                    + "            \"r3\":["
                    + "               {"
                    + "                  \"nodeName\":\"b\","
                    + "                  \"r3._id\":2,"
                    + "                  \"r3.relName\":\"r3\","
                    + "                  \"_type\":\"B\","
                    + "                  \"_id\":1"
                    + "               }"
                    + "            ],"
                    + "            \"_type\":\"B\","
                    + "            \"r1._id\":0,"
                    + "            \"_id\":1,"
                    + "            \"r1.relName\":\"r1\""
                    + "         }"
                    + "      ]"
                    + "   }"
                    + "}";
            assertEquals(parseJson(expectedRow), rows.get(0));
        }
    }

    @Test
    public void testIncomingRelationships() {
        /*          r1:R1         r2:R2
              a:A --------> b:B <------ c:C
        */
        db.executeTransactionally(
                "CREATE (a: A {nodeName: 'a'})-[r1: R1 {relName: 'r1'}]->(b: B {nodeName: 'b'})<-[r2: R2 {relName: 'r2'}]-(c:C {nodeName: 'c'})");

        var query = "MATCH path = (n)-[:R1]->(m)<-[:R2]-(o)\n"
                + "WITH COLLECT(path) AS paths\n"
                + "CALL apoc.paths.toJsonTree(paths, true, {sortPaths: false}) YIELD value AS tree\n"
                + "RETURN tree";

        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(query);
            var rows = result.stream().collect(Collectors.toList());

            assertEquals(rows.size(), 2);
            var expectedFirstRow = "{" + "   \"tree\":{"
                    + "      \"nodeName\":\"a\","
                    + "      \"_type\":\"A\","
                    + "      \"_id\":0,"
                    + "      \"r1\":["
                    + "         {"
                    + "            \"nodeName\":\"b\","
                    + "            \"_type\":\"B\","
                    + "            \"r1._id\":0,"
                    + "            \"_id\":1,"
                    + "            \"r1.relName\":\"r1\""
                    + "         }"
                    + "      ]"
                    + "   }"
                    + "}";
            var expectedSecondRow = "{" + "   \"tree\":{"
                    + "      \"nodeName\":\"c\","
                    + "      \"r2\":["
                    + "         {"
                    + "            \"nodeName\":\"b\","
                    + "            \"r2._id\":1,"
                    + "            \"_type\":\"B\","
                    + "            \"r2.relName\":\"r2\","
                    + "            \"_id\":1"
                    + "         }"
                    + "      ],"
                    + "      \"_type\":\"C\","
                    + "      \"_id\":2"
                    + "   }"
                    + "}";
            assertEquals(parseJson(expectedFirstRow), rows.get(0));
            assertEquals(parseJson(expectedSecondRow), rows.get(1));
        }
    }

    @Test
    public void testToTreeMultiLabelFilters() {
        /*            r:R
              a:A:B -------> c:C
        */
        db.executeTransactionally(
                "CREATE " + "(a: A: B {nodeName: 'a & b'})-[r: R {relName: 'r'}]->(c: C {nodeName: 'c'})");

        var query = "MATCH path = (n)-[r]->(m)\n"
                + "WITH COLLECT(path) AS paths\n"
                + "CALL apoc.paths.toJsonTree(paths, true, {nodes: { A: ['-nodeName'] } }) YIELD value AS tree\n"
                + "RETURN tree";

        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(query);
            var rows = result.stream().collect(Collectors.toList());

            assertEquals(rows.size(), 1);
            // No nodename under A:B
            var expectedRow = "{" + "   \"tree\":{"
                    + "      \"r\":["
                    + "         {"
                    + "            \"nodeName\":\"c\","
                    + "            \"r._id\":0,"
                    + "            \"_type\":\"C\","
                    + "            \"_id\":1,"
                    + "            \"r.relName\":\"r\""
                    + "         }"
                    + "      ],"
                    + "      \"_type\":\"A:B\","
                    + "      \"_id\":0"
                    + "   }"
                    + "}";
            assertEquals(parseJson(expectedRow), rows.get(0));
        }
    }

    @Test
    public void testToTreeMultiLabelFiltersForOldProcedure() {
        /*            r:R
              a:A:B -------> c:C
        */
        db.executeTransactionally(
                "CREATE " + "(a: A: B {nodeName: 'a & b'})-[r: R {relName: 'r'}]->(c: C {nodeName: 'c'})");

        var query = "MATCH path = (n)-[r]->(m)\n"
                + "WITH COLLECT(path) AS paths\n"
                + "CALL apoc.convert.toTree(paths, true, {nodes: { A: ['-nodeName'] } }) YIELD value AS tree\n"
                + "RETURN tree";

        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(query);
            var rows = result.stream().collect(Collectors.toList());

            assertEquals(rows.size(), 1);
            // No nodename under A:B
            var expectedRow = "{" + "   \"tree\":{"
                    + "      \"r\":["
                    + "         {"
                    + "            \"nodeName\":\"c\","
                    + "            \"r._id\":0,"
                    + "            \"_type\":\"C\","
                    + "            \"_id\":1,"
                    + "            \"r.relName\":\"r\""
                    + "         }"
                    + "      ],"
                    + "      \"_type\":\"A:B\","
                    + "      \"_id\":0"
                    + "   }"
                    + "}";
            assertEquals(parseJson(expectedRow), rows.get(0));
        }
    }
}
