package apoc.dv;

import apoc.ApocSettings;
import apoc.create.Create;
import apoc.load.AbstractJdbcTest;
import apoc.load.Jdbc;
import apoc.load.LoadCsv;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static apoc.util.TestUtil.getUrlFileName;
import static apoc.util.TestUtil.isRunningInCI;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallEmpty;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

public class VirtualizeTest extends AbstractJdbcTest{

    public static final Label PERSON = Label.label("Person");

    public static JdbcDatabaseContainer mysql;

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(ApocSettings.apoc_import_file_enabled, true);

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, Virtualize.class, Jdbc.class, LoadCsv.class, Create.class);
    }

    @BeforeClass
    public static void setUpContainer() {
        assumeFalse(isRunningInCI());
        TestUtil.ignoreException(() -> {
            mysql = new MySQLContainer().withInitScript("init_mysql.sql");
            mysql.start();
        },Exception.class);
        assumeNotNull("MySQL container has to exist", mysql);
        assumeTrue("MySQL must be running", mysql.isRunning());
    }

    @AfterClass
    public static void tearDownContainer() {
        if (mysql != null) {
            mysql.stop();
        }
    }

    @Test
    public void testVirtualizeCSV() throws Exception {
        final String name = "csv_vr";
        final String url = getUrlFileName("test.csv").toString();
        final String desc = "person's details";
        final String query = "map.name = $name and map.age = $age";
        List<String> labels = List.of("Person");
        Map<String, Object> map = Map.of("type", "CSV",
                "url", url, "query", query,
                "desc", desc,
                "labels", labels,
                "params", Map.of("header", true));

        final Consumer<Map<String, Object>> assertCatalogContent = (row) -> {
            assertEquals(name, row.get("name"));
            assertEquals(url, row.get("url"));
            assertEquals("CSV", row.get("type"));
            assertEquals(List.of("Person"), row.get("labels"));
            assertEquals(desc, row.get("desc"));
            assertEquals(query, row.get("query"));
        };

        testCall(db, "CALL apoc.dv.catalog.add($name, $map)",
                Map.of("name", name, "map", map),
                assertCatalogContent);

        testCall(db, "CALL apoc.dv.catalog.list()",
                assertCatalogContent);

        String personName = "Rana";
        String personAge = "11";

        Map<String, Object> queryParams = Map.of("name", personName, "age", personAge);
        testCall(db, "CALL apoc.dv.query($name, $queryParams)",
                Map.of("name", name, "queryParams", queryParams),
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(personName, node.getProperty("name"));
                    assertEquals(personAge, node.getProperty("age"));
                    assertEquals(List.of(Label.label("Person")), node.getLabels());
                });

        String hookNodeName = "node to test linking";

        db.executeTransactionally("create (:Hook {name: $hookNodeName})", Map.of("hookNodeName", hookNodeName));

        final String relType = "LINKED_TO";
        testCall(db, "MATCH (hook:Hook) WITH hook " +
                        "CALL apoc.dv.queryAndLink(hook, $relType, $name, $queryParams) yield path " +
                        "RETURN path ",
                Map.of("name", name, "queryParams", queryParams, "relType", relType),
                (row) -> {
                    Path path = (Path) row.get("path");
                    Node node = path.endNode();
                    assertEquals(personName, node.getProperty("name"));
                    assertEquals(personAge, node.getProperty("age"));
                    assertEquals(List.of(Label.label("Person")), node.getLabels());

                    Node hook = path.startNode();
                    assertEquals(hookNodeName, hook.getProperty("name"));
                    assertEquals(List.of(Label.label("Hook")), hook.getLabels());

                    Relationship relationship = path.lastRelationship();
                    assertEquals(hook, relationship.getStartNode());
                    assertEquals(node, relationship.getEndNode());
                    assertEquals(relType, relationship.getType().name());
                });

    }

    @Test
    public void testVirtualizeJDBC() throws Exception {
        String name = "jdbc_vr";
        String desc = "country details";
        List<Label> labels = List.of(Label.label("Country"));
        List<String> labelsAsString = List.of("Country");
        final String query = "SELECT * FROM country WHERE Name = ?";
        final String url = mysql.getJdbcUrl() + "?useSSL=false";
        Map<String, Object> map = Map.of("type", "JDBC",
                "url", url, "query", query,
                "desc", desc,
                "labels", labelsAsString,
                "params", Map.of("credentials", Map.of("user", mysql.getUsername(), "password", mysql.getPassword())));

        testCall(db, "CALL apoc.dv.catalog.add($name, $map)",
                Map.of("name", name, "map", map),
                (row) -> {
                    assertEquals(name, row.get("name"));
                    assertEquals(url, row.get("url"));
                    assertEquals("JDBC", row.get("type"));
                    assertEquals(labelsAsString, row.get("labels"));
                    assertEquals(desc , row.get("desc"));
                });

        testCallEmpty(db, "CALL apoc.dv.query($name, ['Italy'])", Map.of("name", name));

        String country = "Netherlands";
        List<String> queryParams = List.of(country);

        testCall(db, "CALL apoc.dv.query($name, $queryParams)",
                Map.of("name", name, "queryParams", queryParams),
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(country, node.getProperty("Name"));
                    assertEquals(labels, node.getLabels());
                });

        String hookNodeName = "node to test linking";

        db.executeTransactionally("create (:Hook {name: $hookNodeName})", Map.of("hookNodeName", hookNodeName));

        final String relType = "LINKED_TO_NEW";
        testCall(db, "MATCH (hook:Hook) WITH hook " +
                        "CALL apoc.dv.queryAndLink(hook, $relType, $name, $queryParams) yield path " +
                        "RETURN path ",
                Map.of("name", name, "queryParams", queryParams, "relType", relType),
                (row) -> {
                    Path path = (Path) row.get("path");
                    Node node = path.endNode();
                    assertEquals(country, node.getProperty("Name"));
                    assertEquals(labels, node.getLabels());

                    Node hook = path.startNode();
                    assertEquals(hookNodeName, hook.getProperty("name"));
                    assertEquals(List.of(Label.label("Hook")), hook.getLabels());

                    Relationship relationship = path.lastRelationship();
                    assertEquals(hook, relationship.getStartNode());
                    assertEquals(node, relationship.getEndNode());
                    assertEquals(relType, relationship.getType().name());
                });
    }

    @Test
    public void testVirtualizeJDBCWithParameterMap() throws Exception {
        String name = "jdbc_vr";
        String desc = "country details";
        List<Label> labels = List.of(Label.label("Country"));
        List<String> labelsAsString = List.of("Country");
        final String query = "SELECT * FROM country WHERE Name = $name AND HeadOfState = $head_of_state AND Code2 = $CODE2";
        final String url = mysql.getJdbcUrl() + "?useSSL=false";
        Map<String, Object> map = Map.of("type", "JDBC",
                "url", url, "query", query,
                "desc", desc,
                "labels", labelsAsString,
                "params", Map.of("credentials", Map.of("user", mysql.getUsername(), "password", mysql.getPassword())));

        testCall(db, "CALL apoc.dv.catalog.add($name, $map)",
                Map.of("name", name, "map", map),
                (row) -> {
                    assertEquals(name, row.get("name"));
                    assertEquals(url, row.get("url"));
                    assertEquals("JDBC", row.get("type"));
                    assertEquals(labelsAsString, row.get("labels"));
                    assertEquals(desc , row.get("desc"));
                });

        testCallEmpty(db, "CALL apoc.dv.query($name, {name: 'Italy', head_of_state: '', CODE2: ''})", Map.of("name", name));

        String country = "Netherlands";
        String code2 = "NL";
        String headOfState = "Beatrix";
        Map<String, Object> queryParams = Map.of("name", country, "CODE2", code2, "head_of_state", headOfState);

        testCall(db, "CALL apoc.dv.query($name, $queryParams)",
                Map.of("name", name, "queryParams", queryParams),
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(country, node.getProperty("Name"));
                    assertEquals(labels, node.getLabels());
                })Neo4jLogStreamTest;

        String hookNodeName = "node to test linking";

        db.executeTransactionally("create (:Hook {name: $hookNodeName})", Map.of("hookNodeName", hookNodeName));

        final String relType = "LINKED_TO_NEW";
        testCall(db, "MATCH (hook:Hook) WITH hook " +
                        "CALL apoc.dv.queryAndLink(hook, $relType, $name, $queryParams) yield path " +
                        "RETURN path ",
                Map.of("name", name, "queryParams", queryParams, "relType", relType),
                (row) -> {
                    Path path = (Path) row.get("path");
                    Node node = path.endNode();
                    assertEquals(country, node.getProperty("Name"));
                    assertEquals(labels, node.getLabels());

                    Node hook = path.startNode();
                    assertEquals(hookNodeName, hook.getProperty("name"));
                    assertEquals(List.of(Label.label("Hook")), hook.getLabels());

                    Relationship relationship = path.lastRelationship();
                    assertEquals(hook, relationship.getStartNode());
                    assertEquals(node, relationship.getEndNode());
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
                "labels", labelsAsString,
                "params", Map.of("credentials", Map.of("user", mysql.getUsername(), "password", mysql.getPassword())));

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
                "labels", labelsAsString,
                "params", Map.of("credentials", Map.of("user", mysql.getUsername(), "password", mysql.getPassword())));

        db.executeTransactionally("CALL apoc.dv.catalog.add($name, $map)",
                Map.of("name", name, "map", map));
        db.executeTransactionally("CALL apoc.dv.catalog.add($name, $map)",
                Map.of("name", name, "map", map));
        testResult(db, "CALL apoc.dv.catalog.list()",
                Map.of(),
                (result) -> assertEquals(1, result.stream().count()));
    }
}
