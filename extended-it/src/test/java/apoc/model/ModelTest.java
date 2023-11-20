package apoc.model;

import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.util.collection.Iterators;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.graphdb.*;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ModelTest {

    public static String JDBC_URL;
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
        //ApocConfiguration.initialize((GraphDatabaseAPI)db);
        TestUtil.registerProcedure(db, Model.class);
    }

    @Test
    public void testLoadJdbcSchema() {
        testCall(db, "CALL apoc.model.jdbc($url, $config)",
                Util.map("url", mysqlUrl,
                        "config", Util.map("schema", "test",
                                "credentials", Util.map("user", mysql.getUsername(), "password", mysql.getPassword()))),
                (row) -> {
                    Long count = db.executeTransactionally("MATCH (n) RETURN count(n) AS count", Collections.emptyMap(),
                            result -> Iterators.single(result.columnAs("count")));
                    assertEquals(0L, count.longValue());
                    List<Node> nodes = (List<Node>) row.get("nodes");
                    List<Relationship> rels = (List<Relationship>) row.get("relationships");
                    assertEquals( 28, nodes.size());
                    assertEquals( 27, rels.size());

                    // schema
                    Node schema = nodes.stream().filter(node -> node.hasLabel(Label.label("Schema"))).findFirst().orElse(null);
                    assertNotNull("should have schema", schema);
                    assertEquals("test", schema.getProperty("name"));

                    // tables
                    nodes.stream().filter(node -> node.hasLabel(Label.label("Table")))
                            .forEach(table -> {
                                Relationship rel = table.getSingleRelationship(RelationshipType.withName("IN_SCHEMA"), Direction.OUTGOING);
                                assertNotNull("should have relationship IN_SCHEMA", rel);
                                assertEquals(schema, rel.getEndNode());
                            });
                    List<String> tables = nodes.stream().filter(node -> node.hasLabel(Label.label("Table")))
                            .map(node -> node.getProperty("name").toString())
                            .collect(Collectors.toList());
                    assertEquals(3, tables.size());
                    assertEquals(Arrays.asList("country", "city", "countrylanguage"), tables);

                    List<Node> columns = nodes.stream().filter(node -> node.hasLabel(Label.label("Column")))
                            .collect(Collectors.toList());
                    assertEquals(24, columns.size());

                    List<String> countryNodes = filterColumnsByTableName(columns, "country");
                    List<String> expectedCountryCols = Arrays.asList("Code", "Name", "Continent", "Region", "SurfaceArea", "IndepYear", "Population", "LifeExpectancy", "GNP", "GNPOld", "LocalName", "GovernmentForm", "HeadOfState", "Capital", "Code2");
                    assertEquals(expectedCountryCols, countryNodes);

                    List<String> cityNodes = filterColumnsByTableName(columns, "city");
                    List<String> expectedCityCols = Arrays.asList("ID", "Name", "CountryCode", "District", "Population");
                    assertEquals(expectedCityCols, cityNodes);

                    List<String> countrylanguageNodes = filterColumnsByTableName(columns, "countrylanguage");
                    List<String> expectedCountrylanguageCols = Arrays.asList("CountryCode", "Language", "IsOfficial", "Percentage");
                    assertEquals(expectedCountrylanguageCols, countrylanguageNodes);

                });
    }

    @Test
    public void testLoadJdbcSchemaWithWriteOperation() {
        db.executeTransactionally("CALL apoc.model.jdbc($url, $config)",
                Util.map("url", mysqlUrl,
                        "config", Util.map("schema", "test",
                                "write", true,
                                "credentials", Util.map("user", mysql.getUsername(), "password", mysql.getPassword()))),
                innerResult -> Iterators.single(innerResult)
        );

        try (Transaction tx = db.beginTx()) {
            List<Node> nodes = Iterators.single(tx.execute("MATCH (n) RETURN collect(distinct n) AS nodes").columnAs("nodes"));
            List<Relationship> rels = Iterators.single(tx.execute("MATCH ()-[r]-() RETURN collect(distinct r) AS rels").columnAs("rels"));
            assertEquals( 28, nodes.size());
            assertEquals( 27, rels.size());

            // schema
            Node schema = nodes.stream().filter(node -> node.hasLabel(Label.label("Schema"))).findFirst().orElse(null);
            assertNotNull("should have schema", schema);
            assertEquals("test", schema.getProperty("name"));

            // tables
            nodes.stream().filter(node -> node.hasLabel(Label.label("Table")))
                    .forEach(table -> {
                        Relationship rel = table.getSingleRelationship(RelationshipType.withName("IN_SCHEMA"), Direction.OUTGOING);
                        assertNotNull("should have relationship IN_SCHEMA", rel);
                        assertEquals(schema, rel.getEndNode());
                    });
            List<String> tables = nodes.stream().filter(node -> node.hasLabel(Label.label("Table")))
                    .map(node -> node.getProperty("name").toString())
                    .collect(Collectors.toList());
            assertEquals(3, tables.size());
            assertEquals(Arrays.asList("country", "city", "countrylanguage"), tables);

            List<Node> columns = nodes.stream().filter(node -> node.hasLabel(Label.label("Column")))
                    .collect(Collectors.toList());
            assertEquals(24, columns.size());

            List<String> countryNodes = filterColumnsByTableName(columns, "country");
            List<String> expectedCountryCols = Arrays.asList("Code", "Name", "Continent", "Region", "SurfaceArea", "IndepYear", "Population", "LifeExpectancy", "GNP", "GNPOld", "LocalName", "GovernmentForm", "HeadOfState", "Capital", "Code2");
            assertEquals(expectedCountryCols, countryNodes);

            List<String> cityNodes = filterColumnsByTableName(columns, "city");
            List<String> expectedCityCols = Arrays.asList("ID", "Name", "CountryCode", "District", "Population");
            assertEquals(expectedCityCols, cityNodes);

            List<String> countrylanguageNodes = filterColumnsByTableName(columns, "countrylanguage");
            List<String> expectedCountrylanguageCols = Arrays.asList("CountryCode", "Language", "IsOfficial", "Percentage");
            assertEquals(expectedCountrylanguageCols, countrylanguageNodes);

            tx.commit();
        }
    }

    @Test
    public void testLoadJdbcSchemaWithFiltering() {
        testCall(db, "CALL apoc.model.jdbc($url, $config)",
                Util.map("url", mysqlUrl,
                        "config", Util.map("schema", "test",
                                "credentials", Util.map("user", mysql.getUsername(), "password", mysql.getPassword()),
                                "filters", Util.map("tables", Arrays.asList("country\\w*"), "columns", Arrays.asList("(?i)code", "(?i)name", "(?i)Language")))),
                (row) -> {
                    List<Node> nodes = (List<Node>) row.get("nodes");
                    List<Relationship> rels = (List<Relationship>) row.get("relationships");
                    assertEquals( 6, nodes.size());
                    assertEquals( 5, rels.size());

                    // schema
                    Node schema = nodes.stream().filter(node -> node.hasLabel(Label.label("Schema"))).findFirst().orElse(null);
                    assertNotNull("should have schema", schema);
                    assertEquals("test", schema.getProperty("name"));

                    // tables
                    nodes.stream().filter(node -> node.hasLabel(Label.label("Table")))
                            .forEach(table -> {
                                Relationship rel = table.getSingleRelationship(RelationshipType.withName("IN_SCHEMA"), Direction.OUTGOING);
                                assertNotNull("should have relationship IN_SCHEMA", rel);
                                assertEquals(schema, rel.getEndNode());
                            });
                    List<String> tables = nodes.stream().filter(node -> node.hasLabel(Label.label("Table")))
                            .map(node -> node.getProperty("name").toString())
                            .collect(Collectors.toList());
                    assertEquals(2, tables.size());
                    assertEquals(Arrays.asList("country", "countrylanguage"), tables);

                    List<Node> columns = nodes.stream().filter(node -> node.hasLabel(Label.label("Column")))
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
                    Relationship rel = node.getSingleRelationship(RelationshipType.withName("IN_TABLE"), Direction.OUTGOING);
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
