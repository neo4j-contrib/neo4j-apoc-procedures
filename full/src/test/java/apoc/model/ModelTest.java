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
package apoc.model;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import apoc.util.TestUtil;
import apoc.util.Util;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.*;
import org.junit.rules.TestName;
import org.neo4j.graphdb.*;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;

public class ModelTest {

    @Rule
    public TestName testName = new TestName();

    public static JdbcDatabaseContainer mysql;

    private static String mysqlUrl;

    @BeforeClass
    public static void setUpContainer() {
        mysql = new MySQLContainer().withInitScript("init_mysql.sql");
        mysql.start();
        mysqlUrl = mysql.getJdbcUrl() + "?enabledTLSProtocols=TLSv1.2";
    }

    @AfterClass
    public static void tearDownContainer() {
        mysql.stop();
    }

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void initDb() {
        TestUtil.registerProcedure(db, Model.class);
    }

    @After
    public void cleanUp() {
        db.shutdown();
    }

    @Test
    public void testLoadJdbcSchema() {
        testCall(
                db,
                "CALL apoc.model.jdbc($url, $config)",
                Util.map(
                        "url",
                        mysqlUrl,
                        "config",
                        Util.map(
                                "schema",
                                "test",
                                "credentials",
                                Util.map("user", mysql.getUsername(), "password", mysql.getPassword()))),
                (row) -> {
                    Long count = db.executeTransactionally(
                            "MATCH (n) RETURN count(n) AS count",
                            Collections.emptyMap(),
                            result -> Iterators.single(result.columnAs("count")));
                    assertEquals(0L, count.longValue());
                    List<Node> nodes = (List<Node>) row.get("nodes");
                    List<Relationship> rels = (List<Relationship>) row.get("relationships");
                    assertEquals(33, nodes.size());
                    assertEquals(32, rels.size());

                    // schema
                    Node schema = nodes.stream()
                            .filter(node -> node.hasLabel(Label.label("Schema")))
                            .findFirst()
                            .orElse(null);
                    assertNotNull("should have schema", schema);
                    assertEquals("test", schema.getProperty("name"));

                    // tables
                    nodes.stream()
                            .filter(node -> node.hasLabel(Label.label("Table")))
                            .forEach(table -> {
                                Relationship rel = table.getSingleRelationship(
                                        RelationshipType.withName("IN_SCHEMA"), Direction.OUTGOING);
                                assertNotNull("should have relationship IN_SCHEMA", rel);
                                assertEquals(schema, rel.getEndNode());
                            });
                    List<String> tables = nodes.stream()
                            .filter(node -> node.hasLabel(Label.label("Table")))
                            .map(node -> node.getProperty("name").toString())
                            .collect(Collectors.toList());
                    assertEquals(3, tables.size());
                    assertEquals(Arrays.asList("country", "city", "countrylanguage"), tables);

                    List<Node> columns = nodes.stream()
                            .filter(node -> node.hasLabel(Label.label("Column")))
                            .collect(Collectors.toList());
                    assertEquals(29, columns.size());

                    List<String> countryNodes = filterColumnsByTableName(columns, "country");
                    List<String> expectedCountryCols = Arrays.asList("Code", "Name", "Continent", "Region", "SurfaceArea", "IndepYear", "Population", "LifeExpectancy", "GNP", "GNPOld", "LocalName", "GovernmentForm", "HeadOfState", "Capital", "Code2",
                            "myTime", "myDateTime", "myTimeStamp", "myDate", "myYear");
                    assertEquals(expectedCountryCols, countryNodes);

                    List<String> cityNodes = filterColumnsByTableName(columns, "city");
                    List<String> expectedCityCols =
                            Arrays.asList("ID", "Name", "CountryCode", "District", "Population");
                    assertEquals(expectedCityCols, cityNodes);

                    List<String> countrylanguageNodes = filterColumnsByTableName(columns, "countrylanguage");
                    List<String> expectedCountrylanguageCols =
                            Arrays.asList("CountryCode", "Language", "IsOfficial", "Percentage");
                    assertEquals(expectedCountrylanguageCols, countrylanguageNodes);
                });
    }

    @Test
    public void testLoadJdbcSchemaWithWriteOperation() {
        db.executeTransactionally(
                "CALL apoc.model.jdbc($url, $config)",
                Util.map(
                        "url",
                        mysqlUrl,
                        "config",
                        Util.map(
                                "schema",
                                "test",
                                "write",
                                true,
                                "credentials",
                                Util.map("user", mysql.getUsername(), "password", mysql.getPassword()))),
                Iterators::single);

        try (Transaction tx = db.beginTx()) {
            List<Node> nodes = Iterators.single(
                    tx.execute("MATCH (n) RETURN collect(distinct n) AS nodes").columnAs("nodes"));
            List<Relationship> rels = Iterators.single(tx.execute("MATCH ()-[r]-() RETURN collect(distinct r) AS rels")
                    .columnAs("rels"));
            assertEquals(33, nodes.size());
            assertEquals(32, rels.size());

            // schema
            Node schema = nodes.stream()
                    .filter(node -> node.hasLabel(Label.label("Schema")))
                    .findFirst()
                    .orElse(null);
            assertNotNull("should have schema", schema);
            assertEquals("test", schema.getProperty("name"));

            // tables
            nodes.stream().filter(node -> node.hasLabel(Label.label("Table"))).forEach(table -> {
                Relationship rel =
                        table.getSingleRelationship(RelationshipType.withName("IN_SCHEMA"), Direction.OUTGOING);
                assertNotNull("should have relationship IN_SCHEMA", rel);
                assertEquals(schema, rel.getEndNode());
            });
            List<String> tables = nodes.stream()
                    .filter(node -> node.hasLabel(Label.label("Table")))
                    .map(node -> node.getProperty("name").toString())
                    .collect(Collectors.toList());
            assertEquals(3, tables.size());
            assertEquals(Arrays.asList("country", "city", "countrylanguage"), tables);

            List<Node> columns = nodes.stream()
                    .filter(node -> node.hasLabel(Label.label("Column")))
                    .collect(Collectors.toList());
            assertEquals(29, columns.size());

            List<String> countryNodes = filterColumnsByTableName(columns, "country");
            List<String> expectedCountryCols = Arrays.asList("Code", "Name", "Continent", "Region", "SurfaceArea", "IndepYear", "Population", "LifeExpectancy", "GNP", "GNPOld", "LocalName", "GovernmentForm", "HeadOfState", "Capital", "Code2",
                    "myTime", "myDateTime", "myTimeStamp", "myDate", "myYear");
            assertEquals(expectedCountryCols, countryNodes);

            List<String> cityNodes = filterColumnsByTableName(columns, "city");
            List<String> expectedCityCols = Arrays.asList("ID", "Name", "CountryCode", "District", "Population");
            assertEquals(expectedCityCols, cityNodes);

            List<String> countrylanguageNodes = filterColumnsByTableName(columns, "countrylanguage");
            List<String> expectedCountrylanguageCols =
                    Arrays.asList("CountryCode", "Language", "IsOfficial", "Percentage");
            assertEquals(expectedCountrylanguageCols, countrylanguageNodes);

            tx.commit();
        }
    }

    @Test
    public void testLoadJdbcSchemaWithFiltering() {
        testCall(
                db,
                "CALL apoc.model.jdbc($url, $config)",
                Util.map(
                        "url",
                        mysqlUrl,
                        "config",
                        Util.map(
                                "schema",
                                "test",
                                "credentials",
                                Util.map("user", mysql.getUsername(), "password", mysql.getPassword()),
                                "filters",
                                Util.map(
                                        "tables",
                                        Arrays.asList("country\\w*"),
                                        "columns",
                                        Arrays.asList("(?i)code", "(?i)name", "(?i)Language")))),
                (row) -> {
                    List<Node> nodes = (List<Node>) row.get("nodes");
                    List<Relationship> rels = (List<Relationship>) row.get("relationships");
                    assertEquals(6, nodes.size());
                    assertEquals(5, rels.size());

                    // schema
                    Node schema = nodes.stream()
                            .filter(node -> node.hasLabel(Label.label("Schema")))
                            .findFirst()
                            .orElse(null);
                    assertNotNull("should have schema", schema);
                    assertEquals("test", schema.getProperty("name"));

                    // tables
                    nodes.stream()
                            .filter(node -> node.hasLabel(Label.label("Table")))
                            .forEach(table -> {
                                Relationship rel = table.getSingleRelationship(
                                        RelationshipType.withName("IN_SCHEMA"), Direction.OUTGOING);
                                assertNotNull("should have relationship IN_SCHEMA", rel);
                                assertEquals(schema, rel.getEndNode());
                            });
                    List<String> tables = nodes.stream()
                            .filter(node -> node.hasLabel(Label.label("Table")))
                            .map(node -> node.getProperty("name").toString())
                            .collect(Collectors.toList());
                    assertEquals(2, tables.size());
                    assertEquals(Arrays.asList("country", "countrylanguage"), tables);

                    List<Node> columns = nodes.stream()
                            .filter(node -> node.hasLabel(Label.label("Column")))
                            .collect(Collectors.toList());
                    assertEquals(3, columns.size());

                    List<String> countryNodes = filterColumnsByTableName(columns, "country");
                    List<String> expectedCountryCols = Arrays.asList("Code", "Name");
                    assertEquals(expectedCountryCols, countryNodes);

                    List<String> countrylanguageNodes = filterColumnsByTableName(columns, "countrylanguage");
                    List<String> expectedCountrylanguageCols = Arrays.asList("Language");
                    assertEquals(expectedCountrylanguageCols, countrylanguageNodes);
                });
    }

    private List<String> filterColumnsByTableName(List<Node> columns, String tableName) {
        return columns.stream()
                .filter(node -> {
                    Relationship rel =
                            node.getSingleRelationship(RelationshipType.withName("IN_TABLE"), Direction.OUTGOING);
                    if (rel == null) {
                        return false;
                    }
                    String name = rel.getEndNode().getProperty("name").toString();
                    return name.equalsIgnoreCase(tableName);
                })
                .map(node -> node.getProperty("name").toString())
                .collect(Collectors.toList());
    }
}
