package apoc.dv;

import apoc.create.Create;
import apoc.load.Jdbc;
import apoc.load.LoadCsv;
import apoc.util.TestUtil;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.dv.DataVirtualizationCatalogTestUtil.*;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallEmpty;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DataVirtualizationCatalogTest {

    public static JdbcDatabaseContainer mysql;

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, DataVirtualizationCatalog.class, Jdbc.class, LoadCsv.class, Create.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
    }

    @BeforeClass
    public static void setUpContainer() {
        mysql = new MySQLContainer().withInitScript("init_mysql.sql");
        mysql.start();
    }

    @AfterClass
    public static void tearDownContainer() {
        mysql.stop();
    }

    @Test
    public void testVirtualizeCSV() {
        CsvTestResult result = getCsvCommonResult(db);

        final String relType = "LINKED_TO";
        testCall(db, "MATCH (hook:Hook) WITH hook " +
                     "CALL apoc.dv.queryAndLink(hook, $relType, $name, $queryParams, $config) yield path " +
                     "RETURN path ",
                Map.of("name", result.name(), "queryParams", result.queryParams(), "relType", relType, "config", Map.of("header", true)),
                (row) -> {
                    Path path = (Path) row.get("path");
                    Node node = path.endNode();
                    assertEquals(result.personName(), node.getProperty("name"));
                    assertEquals(result.personAge(), node.getProperty("age"));
                    assertEquals(List.of(Label.label("Person")), node.getLabels());

                    Node hook = path.startNode();
                    assertEquals(result.hookNodeName(), hook.getProperty("name"));
                    assertEquals(List.of(Label.label("Hook")), hook.getLabels());

                    Relationship relationship = path.lastRelationship();
                    assertEquals(hook, relationship.getStartNode());
                    assertEquals(node, relationship.getEndNode());
                    assertEquals(relType, relationship.getType().name());
                });

    }
    
    @Test
    public void testVirtualizeCSVWithCustomDirectionIN() {
        CsvTestResult result = getCsvCommonResult(db);
        
        final String relType = "LINKED_TO";
        testCall(db, "MATCH (hook:Hook) WITH hook " +
                     "CALL apoc.dv.queryAndLink(hook, $relType, $name, $queryParams, $config) yield path " +
                     "RETURN path ",
                Map.of("name", result.name(), "queryParams", result.queryParams(), "relType", relType, "config", Map.of("header", true, "direction", "IN")),
                (row) -> {
                    Path path = (Path) row.get("path");
                    Node hook = path.endNode();
                    assertEquals(result.hookNodeName(), hook.getProperty("name"));
                    assertEquals(List.of(Label.label("Hook")), hook.getLabels());
                    Node node = path.startNode();

                    assertEquals(result.personName(), node.getProperty("name"));
                    assertEquals(result.personAge(), node.getProperty("age"));
                    assertEquals(List.of(Label.label("Person")), node.getLabels());

                    Relationship relationship = path.lastRelationship();
                    assertEquals(node, relationship.getStartNode());
                    assertEquals(hook, relationship.getEndNode());
                    assertEquals(relType, relationship.getType().name());
                });

    }

    @Test
    public void testVirtualizeJDBC() {
        VirtualizeJdbcResult result = getVirtualizeJdbcCommonResult(db, mysql);
        
        final String relType = "LINKED_TO_NEW";
        testCall(db, "MATCH (hook:Hook) WITH hook " +
                     "CALL apoc.dv.queryAndLink(hook, $relType, $name, $queryParams, $config) yield path " +
                     "RETURN path ",
                Map.of("name", result.name(), "queryParams", result.queryParams(), "relType", relType,
                        "config", Map.of("credentials", Map.of("user", mysql.getUsername(), "password", mysql.getPassword()))),
                (row) -> {
                    Path path = (Path) row.get("path");
                    Node node = path.endNode();
                    assertEquals(result.country(), node.getProperty("Name"));
                    assertEquals(result.labels(), node.getLabels());

                    Node hook = path.startNode();
                    assertEquals(result.hookNodeName(), hook.getProperty("name"));
                    assertEquals(List.of(Label.label("Hook")), hook.getLabels());

                    Relationship relationship = path.lastRelationship();
                    assertEquals(hook, relationship.getStartNode());
                    assertEquals(node, relationship.getEndNode());
                    assertEquals(relType, relationship.getType().name());
                });
    }

    @Test
    public void testVirtualizeJDBCWithCustomDirectionIN() {
        VirtualizeJdbcResult result = getVirtualizeJdbcCommonResult(db, mysql);

        final String relType = "LINKED_TO_NEW";
        testCall(db, "MATCH (hook:Hook) WITH hook " +
                     "CALL apoc.dv.queryAndLink(hook, $relType, $name, $queryParams, $config) yield path " +
                     "RETURN path ",
                Map.of("name", result.name(), "queryParams", result.queryParams(), "relType", relType,
                        "config", Map.of(
                                "credentials", Map.of("user", mysql.getUsername(), "password", mysql.getPassword()),
                                "direction", "IN"
                        )),
                (row) -> {
                    Path path = (Path) row.get("path");
                    Node hook = path.endNode();
                    assertEquals(result.hookNodeName(), hook.getProperty("name"));
                    assertEquals(List.of(Label.label("Hook")), hook.getLabels());

                    Node node = path.startNode();
                    assertEquals(result.country(), node.getProperty("Name"));
                    assertEquals(result.labels(), node.getLabels());

                    Relationship relationship = path.lastRelationship();
                    assertEquals(node, relationship.getStartNode());
                    assertEquals(hook, relationship.getEndNode());
                    assertEquals(relType, relationship.getType().name());
                });
    }

    @Test
    public void testVirtualizeJDBCWithParameterMap() {
        VirtualizeJdbcWithParameterResult result = getVirtualizeJdbcWithParamsCommonResult(db, mysql);

        final String relType = "LINKED_TO_NEW";
        testCall(db, "MATCH (hook:Hook) WITH hook " +
                        "CALL apoc.dv.queryAndLink(hook, $relType, $name, $queryParams, $config) yield path " +
                        "RETURN path ",
                Map.of("name", result.name(), "queryParams", result.queryParams(), "relType", relType,
                        "config", Map.of("credentials", Map.of("user", mysql.getUsername(), "password", mysql.getPassword()))),
                (row) -> {
                    Path path = (Path) row.get("path");
                    Node node = path.endNode();
                    assertEquals(result.country(), node.getProperty("Name"));
                    assertEquals(result.labels(), node.getLabels());

                    Node hook = path.startNode();
                    assertEquals(result.hookNodeName(), hook.getProperty("name"));
                    assertEquals(List.of(Label.label("Hook")), hook.getLabels());

                    Relationship relationship = path.lastRelationship();
                    assertEquals(hook, relationship.getStartNode());
                    assertEquals(node, relationship.getEndNode());
                    assertEquals(relType, relationship.getType().name());
                });
    }
    

    @Test
    public void testVirtualizeJDBCWithParameterMapAndDirectionIN() {
        VirtualizeJdbcWithParameterResult result = getVirtualizeJdbcWithParamsCommonResult(db, mysql);

        final String relType = "LINKED_TO_NEW";
        testCall(db, "MATCH (hook:Hook) WITH hook " +
                     "CALL apoc.dv.queryAndLink(hook, $relType, $name, $queryParams, $config) yield path " +
                     "RETURN path ",
                Map.of("name", result.name(), "queryParams", result.queryParams(), "relType", relType,
                        "config", Map.of("credentials", Map.of("user", mysql.getUsername(), "password", mysql.getPassword()),
                                "direction", "IN"
                        )),
                (row) -> {
                    Path path = (Path) row.get("path");
                    Node hook = path.endNode();
                    assertEquals(result.hookNodeName(), hook.getProperty("name"));
                    assertEquals(List.of(Label.label("Hook")), hook.getLabels());

                    Node node = path.startNode();
                    assertEquals(result.country(), node.getProperty("Name"));
                    assertEquals(result.labels(), node.getLabels());

                    Relationship relationship = path.lastRelationship();
                    assertEquals(node, relationship.getStartNode());
                    assertEquals(hook, relationship.getEndNode());
                    assertEquals(relType, relationship.getType().name());
                });
    }

    @Test
    public void testRemove() {
        String name = "jdbc_vr";
        String desc = "country details";
        List<String> labelsAsString = List.of("Country");
        final String query = "SELECT * FROM country WHERE Name = $name";
        final String url = mysql.getJdbcUrl() + "?useSSL=false";
        Map<String, Object> map = Map.of("type", "JDBC",
                "url", url, "query", query,
                "desc", desc,
                "labels", labelsAsString);

        db.executeTransactionally("CALL apoc.dv.catalog.add($name, $map)",
                Map.of("name", name, "map", map));

        testCallEmpty(db, "CALL apoc.dv.catalog.remove($name)", Map.of("name", name));
    }

    @Test
    public void testNameAsKey() {
        String name = "jdbc_vr";
        String desc = "country details";
        List<String> labelsAsString = List.of("Country");
        final String query = "SELECT * FROM country WHERE Name = $name";
        final String url = mysql.getJdbcUrl() + "?useSSL=false";
        Map<String, Object> map = Map.of("type", "JDBC",
                "url", url, "query", query,
                "desc", desc,
                "labels", labelsAsString);

        db.executeTransactionally("CALL apoc.dv.catalog.add($name, $map)",
                Map.of("name", name, "map", map));
        db.executeTransactionally("CALL apoc.dv.catalog.add($name, $map)",
                Map.of("name", name, "map", map));
        testResult(db, "CALL apoc.dv.catalog.list()",
                Map.of(),
                (result) -> assertEquals(1, result.stream().count()));
    }

    @Test
    public void testJDBCQueryWithMixedParamsTypes() {
        try {
            String name = "jdbc_vr";
            String desc = "country details";
            List<String> labelsAsString = List.of("Country");
            final String query = "SELECT * FROM country WHERE Name = $name AND param_with_question_mark = ? ";
            final String url = mysql.getJdbcUrl() + "?useSSL=false";
            Map<String, Object> map = Map.of("type", "JDBC",
                    "url", url, "query", query,
                    "desc", desc,
                    "labels", labelsAsString);

            db.executeTransactionally("CALL apoc.dv.catalog.add($name, $map)",
                    Map.of("name", name, "map", map));
            Assert.fail("Exception is expected");
        } catch (Exception e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            assertTrue(rootCause instanceof IllegalArgumentException);
            assertEquals("The query is mixing parameters with `$` and `?` please use just one notation", rootCause.getMessage());
        }
    }

    @Test
    public void testVirtualizeJDBCWithDifferentParameterMap() {
        String name = "jdbc_vr";
        String desc = "country details";
        List<Label> labels = List.of(Label.label("Country"));
        List<String> labelsAsString = List.of("Country");
        final String query = "SELECT * FROM country WHERE Name = $name AND HeadOfState = $head_of_state AND Code2 = $CODE2";
        final String url = mysql.getJdbcUrl() + "?useSSL=false";
        Map<String, Object> map = Map.of("type", "JDBC",
                "url", url, "query", query,
                "desc", desc,
                "labels", labelsAsString);

        final List<String> expectedParams = List.of("$name", "$head_of_state", "$CODE2");
        final List<String> sortedExpectedParams = expectedParams.stream()
                .sorted()
                .collect(Collectors.toList());
        testCall(db, "CALL apoc.dv.catalog.add($name, $map)",
                Map.of("name", name, "map", map),
                (row) -> {
                    assertEquals(name, row.get("name"));
                    assertEquals(url, row.get("url"));
                    assertEquals("JDBC", row.get("type"));
                    assertEquals(labelsAsString, row.get("labels"));
                    assertEquals(desc , row.get("desc"));
                    assertEquals(expectedParams, row.get("params"));
                });

        String country = "Netherlands";
        String code2 = "NL";
        String headOfState = "Beatrix";
        Map<String, Object> queryParams = Map.of("foo", country, "bar", code2, "baz", headOfState);

        try {
            db.executeTransactionally("CALL apoc.dv.query($name, $queryParams, $config)",
                    Map.of("name", name, "queryParams", queryParams,
                            "config", Map.of("credentials", Map.of("user", mysql.getUsername(), "password", mysql.getPassword()))),
                    Result::resultAsString);
            Assert.fail("Exception is expected");
        } catch (Exception e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            assertTrue(rootCause instanceof IllegalArgumentException);
            final List<String> actualParams = queryParams.keySet().stream()
                    .map(s -> "$" + s)
                    .sorted()
                    .collect(Collectors.toList());
            assertEquals(String.format("Expected query parameters are %s, actual are %s", sortedExpectedParams, actualParams), rootCause.getMessage());
        }
    }
}
