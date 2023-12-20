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
package apoc.neighbors;

import static org.junit.Assert.assertEquals;

import apoc.util.TestUtil;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

public class NeighborsTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, Neighbors.class);
        db.executeTransactionally("CREATE (a:First), " + "(b:Neighbor{name: 'b'}), "
                + "(c:Neighbor{name: 'c'}), "
                + "(d:Neighbor{name: 'd'}), "
                + "(a)-[:KNOWS]->(b), "
                + "(b)-[:KNOWS]->(a), "
                + "(b)-[:KNOWS]->(c), "
                + "(c)-[:KNOWS]->(d) ");
    }

    @After
    public void teardown() {
        db.shutdown();
    }

    @Test
    public void getNeighbors2Hops() {
        TestUtil.testCall(
                db,
                "MATCH (n:First) WITH n " + "CALL apoc.neighbors.tohop(n,'KNOWS>', 2) YIELD node AS neighbor "
                        + "RETURN COLLECT(neighbor) AS neighbors",
                (row) -> {
                    List<Node> neighbors = (List<Node>) row.get("neighbors");
                    assertEquals(2, neighbors.size());
                    assertEquals(
                            Arrays.asList("b", "c"),
                            neighbors.stream().map(n -> n.getProperty("name")).collect(Collectors.toList()));
                });
    }

    @Test
    public void getNeighbors3Hops() {
        TestUtil.testCall(
                db,
                "MATCH (n:First) WITH n " + "CALL apoc.neighbors.tohop(n,'KNOWS>', 3) YIELD node AS neighbor "
                        + "RETURN COLLECT(neighbor) AS neighbors",
                (row) -> {
                    List<Node> neighbors = (List<Node>) row.get("neighbors");
                    assertEquals(3, neighbors.size());
                    assertEquals(
                            Arrays.asList("b", "c", "d"),
                            neighbors.stream().map(n -> n.getProperty("name")).collect(Collectors.toList()));
                });
    }

    @Test
    public void getNeighborsCount2Hops() {
        TestUtil.testCall(
                db,
                "MATCH (n:First) WITH n " + "CALL apoc.neighbors.tohop.count(n,'KNOWS>', 2) YIELD value AS number "
                        + "RETURN number",
                (row) -> assertEquals(2L, row.get("number")));
    }

    @Test
    public void getNeighborsCount3Hops() {
        TestUtil.testCall(
                db,
                "MATCH (n:First) WITH n " + "CALL apoc.neighbors.tohop.count(n,'KNOWS>', 3) YIELD value AS number "
                        + "RETURN number",
                (row) -> assertEquals(3L, row.get("number")));
    }

    @Test
    public void getNeighborsByHop2Hops() {
        TestUtil.testCall(
                db,
                "MATCH (n:First) WITH n " + "CALL apoc.neighbors.byhop(n,'KNOWS>', 2) YIELD nodes AS neighbor "
                        + "RETURN COLLECT(neighbor) AS neighbors",
                (row) -> {
                    List<List<Node>> neighbors = (List<List<Node>>) row.get("neighbors");
                    assertEquals(2, neighbors.size());
                    assertEquals(
                            Arrays.asList(Arrays.asList("b"), Arrays.asList("c")),
                            neighbors.stream()
                                    .map(l -> l.stream()
                                            .map(n -> n.getProperty("name"))
                                            .collect(Collectors.toList()))
                                    .collect(Collectors.toList()));
                });
    }

    @Test
    public void getNeighborsByHop3Hops() {
        TestUtil.testCall(
                db,
                "MATCH (n:First) WITH n " + "CALL apoc.neighbors.byhop(n,'KNOWS>', 3) YIELD nodes AS neighbor "
                        + "RETURN COLLECT(neighbor) AS neighbors",
                (row) -> {
                    List<List<Node>> neighbors = (List<List<Node>>) row.get("neighbors");
                    assertEquals(3, neighbors.size());
                    assertEquals(
                            Arrays.asList(Arrays.asList("b"), Arrays.asList("c"), Arrays.asList("d")),
                            neighbors.stream()
                                    .map(l -> l.stream()
                                            .map(n -> n.getProperty("name"))
                                            .collect(Collectors.toList()))
                                    .collect(Collectors.toList()));
                });
    }

    @Test
    public void getNeighborsByHopCount2Hops() {
        TestUtil.testCall(
                db,
                "MATCH (n:First) WITH n " + "CALL apoc.neighbors.byhop.count(n,'KNOWS>', 2) YIELD value AS numbers "
                        + "RETURN numbers",
                (row) -> {
                    List<Long> numbers = (List<Long>) row.get("numbers");
                    assertEquals(2, numbers.size());
                });
    }

    @Test
    public void getNeighborsByHopCount3Hops() {
        TestUtil.testCall(
                db,
                "MATCH (n:First) WITH n " + "CALL apoc.neighbors.byhop.count(n,'KNOWS>', 3) YIELD value AS numbers "
                        + "RETURN numbers",
                (row) -> {
                    List<Long> numbers = (List<Long>) row.get("numbers");
                    assertEquals(3, numbers.size());
                });
    }

    @Test
    public void getNeighborsAt2Hops() {
        TestUtil.testCall(
                db,
                "MATCH (n:First) WITH n " + "CALL apoc.neighbors.athop(n,'KNOWS>', 2) YIELD node AS neighbor "
                        + "RETURN COLLECT(neighbor) AS neighbors",
                (row) -> {
                    List<Node> neighbors = (List<Node>) row.get("neighbors");
                    assertEquals(1, neighbors.size());
                    assertEquals(
                            Arrays.asList("c"),
                            neighbors.stream().map(n -> n.getProperty("name")).collect(Collectors.toList()));
                });
    }

    @Test
    public void getNeighborsAt3Hops() {
        TestUtil.testCall(
                db,
                "MATCH (n:First) WITH n " + "CALL apoc.neighbors.athop(n,'KNOWS>', 3) YIELD node AS neighbor "
                        + "RETURN COLLECT(neighbor) AS neighbors",
                (row) -> {
                    List<Node> neighbors = (List<Node>) row.get("neighbors");
                    assertEquals(1, neighbors.size());
                    assertEquals(
                            Arrays.asList("d"),
                            neighbors.stream().map(n -> n.getProperty("name")).collect(Collectors.toList()));
                });
    }

    @Test
    public void getNeighborsCountAt2Hops() {
        TestUtil.testCall(
                db,
                "MATCH (n:First) WITH n " + "CALL apoc.neighbors.athop.count(n,'KNOWS>', 2) YIELD value AS number "
                        + "RETURN number",
                (row) -> assertEquals(1L, row.get("number")));
    }

    @Test
    public void getNeighborsCountAt3Hops() {
        TestUtil.testCall(
                db,
                "MATCH (n:First) WITH n " + "CALL apoc.neighbors.athop.count(n,'KNOWS>', 3) YIELD value AS number "
                        + "RETURN number",
                (row) -> assertEquals(1L, row.get("number")));
    }
}
