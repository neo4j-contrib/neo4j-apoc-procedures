package apoc.dv;

import org.jetbrains.annotations.NotNull;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static apoc.util.TestUtil.getUrlFileName;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallEmpty;
import static org.junit.Assert.assertEquals;

public class DataVirtualizationCatalogTestUtil {

    record CsvTestResult(String name, String personName, String personAge, String hookNodeName, Map<String, Object> queryParams) { }

    static CsvTestResult getCsvCommonResult(GraphDatabaseService db) {
        final String name = "csv_vr";
        final String url = getUrlFileName("test.csv").toString();
        final String desc = "person's details";
        final String query = "map.name = $name and map.age = $age";
        List<String> labels = List.of("Person");
        Map<String, Object> map = Map.of("type", "CSV",
                "url", url, "query", query,
                "desc", desc,
                "labels", labels);

        String personName = "Rana";
        String personAge = "11";
        String hookNodeName = "node to test linking";

        final Consumer<Map<String, Object>> assertCatalogContent = (row) -> {
            assertEquals(name, row.get("name"));
            assertEquals(url, row.get("url"));
            assertEquals("CSV", row.get("type"));
            assertEquals(List.of("Person"), row.get("labels"));
            assertEquals(desc, row.get("desc"));
            assertEquals(query, row.get("query"));
            assertEquals(List.of("$name", "$age"), row.get("params"));
        };

        testCall(db, "CALL apoc.dv.catalog.add($name, $map)",
                Map.of("name", name, "map", map),
                assertCatalogContent);

        testCall(db, "CALL apoc.dv.catalog.list()",
                assertCatalogContent);


        Map<String, Object> queryParams = Map.of("name", personName, "age", personAge);
        testCall(db, "CALL apoc.dv.query($name, $queryParams, $config)",
                Map.of("name", name, "queryParams", queryParams, "config", Map.of("header", true)),
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(personName, node.getProperty("name"));
                    assertEquals(personAge, node.getProperty("age"));
                    assertEquals(List.of(Label.label("Person")), node.getLabels());
                });


        db.executeTransactionally("create (:Hook {name: $hookNodeName})", Map.of("hookNodeName", hookNodeName));
        return new CsvTestResult(name, personName, personAge, hookNodeName, queryParams);
    }

    record VirtualizeJdbcResult(String name, List<Label> labels, String country, List<String> queryParams, String hookNodeName) {}

    static VirtualizeJdbcResult getVirtualizeJdbcCommonResult(GraphDatabaseService db, JdbcDatabaseContainer mysql) {
        String name = "jdbc_vr";
        String desc = "country details";
        List<Label> labels = List.of(Label.label("Country"));
        List<String> labelsAsString = List.of("Country");
        final String query = "SELECT * FROM country WHERE Name = ?";
        final String url = mysql.getJdbcUrl() + "?useSSL=false";
        Map<String, Object> map = Map.of("type", "JDBC",
                "url", url, "query", query,
                "desc", desc,
                "labels", labelsAsString);

        testCall(db, "CALL apoc.dv.catalog.add($name, $map)",
                Map.of("name", name, "map", map),
                (row) -> {
                    assertEquals(name, row.get("name"));
                    assertEquals(url, row.get("url"));
                    assertEquals("JDBC", row.get("type"));
                    assertEquals(labelsAsString, row.get("labels"));
                    assertEquals(desc, row.get("desc"));
                    assertEquals(List.of("?"), row.get("params"));
                });

        testCallEmpty(db, "CALL apoc.dv.query($name, ['Italy'], $config)", Map.of("name", name,
                "config", Map.of("credentials", Map.of("user", mysql.getUsername(), "password", mysql.getPassword()))));

        String country = "Netherlands";
        List<String> queryParams = List.of(country);

        testCall(db, "CALL apoc.dv.query($name, $queryParams, $config)",
                Map.of("name", name, "queryParams", queryParams,
                        "config", Map.of("credentials", Map.of("user", mysql.getUsername(), "password", mysql.getPassword()))),
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(country, node.getProperty("Name"));
                    assertEquals(labels, node.getLabels());
                });

        String hookNodeName = "node to test linking";

        db.executeTransactionally("create (:Hook {name: $hookNodeName})", Map.of("hookNodeName", hookNodeName));
        return new VirtualizeJdbcResult(name, labels, country, queryParams, hookNodeName);
    }
    
    record VirtualizeJdbcWithParameterResult(String name, List<Label> labels, String country, Map<String, Object> queryParams, String hookNodeName) { }
    
    static VirtualizeJdbcWithParameterResult getVirtualizeJdbcWithParamsCommonResult(GraphDatabaseService db, JdbcDatabaseContainer mysql) throws QueryExecutionException {
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

        testCall(db, "CALL apoc.dv.catalog.add($name, $map)",
                Map.of("name", name, "map", map),
                (row) -> {
                    assertEquals(name, row.get("name"));
                    assertEquals(url, row.get("url"));
                    assertEquals("JDBC", row.get("type"));
                    assertEquals(labelsAsString, row.get("labels"));
                    assertEquals(desc , row.get("desc"));
                    assertEquals(List.of("$name", "$head_of_state", "$CODE2"), row.get("params"));
                });

        testCallEmpty(db, "CALL apoc.dv.query($name, {name: 'Italy', head_of_state: '', CODE2: ''}, $config)",
                Map.of("name", name, "config", Map.of("credentials", Map.of("user", mysql.getUsername(), "password", mysql.getPassword()))));

        String country = "Netherlands";
        String code2 = "NL";
        String headOfState = "Beatrix";
        Map<String, Object> queryParams = Map.of("name", country, "CODE2", code2, "head_of_state", headOfState);

        testCall(db, "CALL apoc.dv.query($name, $queryParams, $config)",
                Map.of("name", name, "queryParams", queryParams,
                        "config", Map.of("credentials", Map.of("user", mysql.getUsername(), "password", mysql.getPassword()))),
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(country, node.getProperty("Name"));
                    assertEquals(labels, node.getLabels());
                });

        String hookNodeName = "node to test linking";

        db.executeTransactionally("create (:Hook {name: $hookNodeName})", Map.of("hookNodeName", hookNodeName));
        return new VirtualizeJdbcWithParameterResult(name, labels, country, queryParams, hookNodeName);
    }

}
