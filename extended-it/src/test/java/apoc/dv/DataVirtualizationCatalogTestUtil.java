package apoc.dv;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.util.List;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static apoc.util.SystemDbTestUtil.TIMEOUT;
import static apoc.util.TestUtil.getUrlFileName;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallCount;
import static apoc.util.TestUtil.testCallEmpty;
import static apoc.util.TestUtil.testCallEventually;
import static org.junit.Assert.assertEquals;

public class DataVirtualizationCatalogTestUtil {

    public static final String AGE_KEY = "age";
    private static final String MAP_KEY = "map";
    public static final String APOC_DV_QUERY_PARAMS_KEY = "queryParams";
    public static final String CONFIG_KEY = "config";
    public static final Map<String, Object> CONFIG_VALUE = map("header", true);
    public static final String CSV_NAME_VALUE = "csv_vr";
    public static final String CSV_TEST_FILE = "test.csv";
    public static final String DATABASE_NAME = "databaseName";
    public static final String DESC_KEY = "desc";
    public static final String DESC_VALUE = "person's details";
    public static final List<String> EXPECTED_LIST = List.of("$name", "$head_of_state", "$CODE2");
    public static final List<String> EXPECTED_LIST_SORTED = List.of("$name", "$head_of_state", "$CODE2").stream().sorted().toList();
    public static final String FILE_URL = getUrlFileName(CSV_TEST_FILE).toString();
    public static final String HOOK_NODE_NAME_KEY = "hookNodeName";
    public static final String HOOK_NODE_NAME_VALUE = "node to test linking";
    public static final String JDBC_VALUE = "JDBC";
    public static final String HOOK_LABEL = "Hook";
    public static final String CREDENTIALS_KEY = "credentials";
    public static List<String> LABELS = List.of("Person");
    public static final String LABELS_KEY = "labels";
    public static final String LABELS_VALUE = "Person";
    public static final String NAME_KEY = "name";
    public static final String NODE_KEY = "node";
    public static final String PARAMS_KEY = "params";
    public static final List<String> PARAMS_VALUE = List.of("$name", "$age");
    public static final String PERSON_NAME = "Rana";
    public static final String PERSON_AGE = "11";
    public static final String QUERY_KEY = "query";
    public static final String QUERY_VALUE = "map.name = $name and map.age = $age";
    public static final String RELTYPE_KEY = "relType";
    public static final String RELTYPE_VALUE = "LINKED_TO";
    public static final String TYPE_KEY = "type";
    public static final String TYPE_VALUE = "CSV";
    public static final String URL_KEY = "url";
    public static Map<String, Object> MAP_VALUE = map("type", "CSV",
            "url", CSV_TEST_FILE, "query", QUERY_VALUE,
            "desc", DESC_VALUE,
            "labels", LABELS);
    // Virtualize JDBC
    public static final String VIRTUALIZE_JDBC_QUERY = "SELECT * FROM country WHERE Name = ?";
    public static final String JDBC_SELECT_QUERY = "SELECT * FROM country WHERE Name = $name";
    public static final String JDBC_SELECT_QUERY_WITH_PARAM = "SELECT * FROM country WHERE Name = $name AND param_with_question_mark = ? ";

    // Virtualize JDBC With Params Map
    public static final String VIRTUALIZE_JDBC_COUNTRY = "Netherlands";
    public static final String CODE2 = "NL";
    public static final String HEAD_OF_STATE = "Beatrix";
    public static final List<String> VIRTUALIZE_JDBC_APOC_PARAMS = List.of(VIRTUALIZE_JDBC_COUNTRY);
    public static final Map<String, Object> VIRTUALIZE_JDBC_QUERY_PARAMS = map(NAME_KEY, VIRTUALIZE_JDBC_COUNTRY, "CODE2", CODE2, "head_of_state", HEAD_OF_STATE);
    public static final Map<String, Object> VIRTUALIZE_JDBC_QUERY_WRONG_PARAMS = map("foo", VIRTUALIZE_JDBC_COUNTRY, "bar", CODE2, "baz", HEAD_OF_STATE);
    public static final String VIRTUALIZE_JDBC_WITH_PARAMS_RELTYPE = "LINKED_TO_NEW";
    public static final String JDBC_NAME = "jdbc_vr";
    public static final String JDBC_DESC = "country details";
    public static final List<Label> JDBC_LABELS = List.of(Label.label("Country"));
    public static final List<String> JDBC_LABELS_AS_STRING = List.of("Country");
    public static final String JDBC_SSL_CONFIG = "?useSSL=false";

    public static final String VIRTUALIZE_JDBC_WITH_PARAMS_QUERY = "SELECT * FROM country WHERE Name = $name AND HeadOfState = $head_of_state AND Code2 = $CODE2";
    public static final String APOC_DV_JDBC_WITH_PARAMS_QUERY = "CALL apoc.dv.query($name, {name: 'Italy', head_of_state: '', CODE2: ''}, $config)";


    public static final String CREATE_HOOK_QUERY = "create (:Hook {name: $hookNodeName})";
    public static final Map<String, Object> CREATE_HOOK_PARAMS = map(HOOK_NODE_NAME_KEY, HOOK_NODE_NAME_VALUE);

    
    // APOC Queries
    public static final String APOC_DV_ADD_QUERY = "CALL apoc.dv.catalog.add($name, $map)";
    public static final String APOC_DV_LIST = "CALL apoc.dv.catalog.list()";
    public static final String APOC_DV_DROP_QUERY = "CALL apoc.dv.catalog.drop($name, $databaseName)";
    public static final String APOC_DV_INSTALL_QUERY = "CALL apoc.dv.catalog.install($name, $databaseName, $map)";
    public static final Map<String, Object> APOC_DV_INSTALL_PARAMS = map(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME, "name", CSV_NAME_VALUE, "map", MAP_VALUE);
    public static final String APOC_DV_QUERY = "CALL apoc.dv.query($name, $queryParams, $config)";
    public static final String APOC_DV_QUERY_WITH_PARAM = "CALL apoc.dv.query($name, ['Italy'], $config)";
    public static final String APOC_DV_QUERY_AND_LINK_QUERY = "MATCH (hook:Hook) WITH hook " +
                                                              "CALL apoc.dv.queryAndLink(hook, $relType, $name, $queryParams, $config) yield path " +
                                                              "RETURN path ";
    public static final String APOC_DV_SHOW_QUERY = "CALL apoc.dv.catalog.show()";
    
    public static Map<String, Object> APOC_DV_QUERY_PARAMS = map(NAME_KEY, PERSON_NAME, AGE_KEY, PERSON_AGE);
    public static final Map<String, Object> APOC_DV_DROP_PARAMS = map(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,"name", CSV_NAME_VALUE);
    public static final Map<String, Object> APOC_DV_QUERY_AND_LINK_QUERY_PARAMS = map(NAME_KEY, CSV_NAME_VALUE, APOC_DV_QUERY_PARAMS_KEY, APOC_DV_QUERY_PARAMS, RELTYPE_KEY, RELTYPE_VALUE, CONFIG_KEY, CONFIG_VALUE);

    public static Map<String, Object> withDirectionIn(Map<String, Object> config) {
        config.put("direction", "IN");
        return config;
    }


    public static Map<String, Object> getAddQueryConfigMap(String url) {
        return map("type", "CSV",
                "url", url, "query", QUERY_VALUE,
                "desc", DESC_VALUE,
                "labels", LABELS);
    }

    public static Map<String, Object> getJdbcCredentials(JdbcDatabaseContainer mysql) {
        return map("user", mysql.getUsername(), "password", mysql.getPassword());
    }

    public static void assertCatalogContent(Map<String, Object> row, String url) {
        assertEquals(CSV_NAME_VALUE, row.get(NAME_KEY));
        assertEquals(url, row.get(URL_KEY));
        assertEquals(TYPE_VALUE, row.get(TYPE_KEY));
        assertEquals(List.of(LABELS_VALUE), row.get(LABELS_KEY));
        assertEquals(DESC_VALUE, row.get(DESC_KEY));
        assertEquals(QUERY_VALUE, row.get(QUERY_KEY));
        assertEquals(PARAMS_VALUE, row.get(PARAMS_KEY));
    };

    public static void assertDvCatalogAddOrInstall(Map<String, Object> row, String url) {
        assertEquals(JDBC_NAME, row.get(NAME_KEY));
        assertEquals(url, row.get(URL_KEY));
        assertEquals(JDBC_VALUE, row.get(TYPE_KEY));
        assertEquals(JDBC_LABELS_AS_STRING, row.get(LABELS_KEY));
        assertEquals(JDBC_DESC, row.get(DESC_KEY));
        assertEquals(EXPECTED_LIST, row.get(PARAMS_KEY));
    }

    public static void assertDvQueryAndLinkContent(Map<String, Object> row) {
        Path path = (Path) row.get("path");
        Node node = path.endNode();
        assertEquals(VIRTUALIZE_JDBC_COUNTRY, node.getProperty("Name"));
        assertEquals(JDBC_LABELS, node.getLabels());

        Node hook = path.startNode();
        assertEquals(HOOK_NODE_NAME_VALUE, hook.getProperty("name"));
        assertEquals(List.of(Label.label(HOOK_LABEL)), hook.getLabels());

        Relationship relationship = path.lastRelationship();
        assertEquals(hook, relationship.getStartNode());
        assertEquals(node, relationship.getEndNode());
        assertEquals(VIRTUALIZE_JDBC_WITH_PARAMS_RELTYPE, relationship.getType().name());
    }

    public static void assertDvQueryAndLinkContentDirectionIN(Map<String, Object> row) {
        Path path = (Path) row.get("path");
        Node hook = path.endNode();
        assertEquals(HOOK_NODE_NAME_VALUE, hook.getProperty("name"));
        assertEquals(List.of(Label.label(HOOK_LABEL)), hook.getLabels());

        Node node = path.startNode();
        assertEquals(VIRTUALIZE_JDBC_COUNTRY, node.getProperty("Name"));
        assertEquals(JDBC_LABELS, node.getLabels());

        Relationship relationship = path.lastRelationship();
        assertEquals(node, relationship.getStartNode());
        assertEquals(hook, relationship.getEndNode());
        assertEquals(VIRTUALIZE_JDBC_WITH_PARAMS_RELTYPE, relationship.getType().name());
    }

    public static void assertDvQueryContent(Map<String, Object> row, String url) {
        assertEquals(JDBC_NAME, row.get(NAME_KEY));
        assertEquals(url, row.get(URL_KEY));
        assertEquals(JDBC_VALUE, row.get(TYPE_KEY));
        assertEquals(JDBC_LABELS_AS_STRING, row.get(LABELS_KEY));
        assertEquals(JDBC_DESC, row.get(DESC_KEY));
        assertEquals(List.of("?"), row.get(PARAMS_KEY));
    }

    public static void assertVirtualizeCSVQueryAndLinkContent(Map<String, Object> row) {
        Path path = (Path) row.get("path");
        Node node = path.endNode();
        assertEquals(PERSON_NAME, node.getProperty(NAME_KEY));
        assertEquals(PERSON_AGE, node.getProperty(AGE_KEY));
        assertEquals(List.of(Label.label(LABELS_VALUE)), node.getLabels());

        Node hook = path.startNode();
        assertEquals(HOOK_NODE_NAME_VALUE, hook.getProperty(NAME_KEY));
        assertEquals(List.of(Label.label(HOOK_LABEL)), hook.getLabels());

        Relationship relationship = path.lastRelationship();
        assertEquals(hook, relationship.getStartNode());
        assertEquals(node, relationship.getEndNode());
        assertEquals(RELTYPE_VALUE, relationship.getType().name());
    }

    public static void assertVirtualizeCSVQueryAndLinkContentDirectionIN(Map<String, Object> row) {
        Path path = (Path) row.get("path");
        Node hook = path.endNode();
        assertEquals(HOOK_NODE_NAME_VALUE, hook.getProperty(NAME_KEY));
        assertEquals(List.of(Label.label(HOOK_LABEL)), hook.getLabels());

        Node node = path.startNode();
        assertEquals(PERSON_NAME, node.getProperty(NAME_KEY));
        assertEquals(PERSON_AGE, node.getProperty(AGE_KEY));
        assertEquals(List.of(Label.label(LABELS_VALUE)), node.getLabels());

        Relationship relationship = path.lastRelationship();
        assertEquals(node, relationship.getStartNode());
        assertEquals(hook, relationship.getEndNode());
        assertEquals(RELTYPE_VALUE, relationship.getType().name());
    }

    static void assertDVQueryVirtualizeCSV(Map<String, Object> row) {
        Node node = (Node) row.get(NODE_KEY);
        assertEquals(PERSON_NAME, node.getProperty(NAME_KEY));
        assertEquals(PERSON_AGE, node.getProperty(AGE_KEY));
        assertEquals(List.of(Label.label("Person")), node.getLabels());
    }

    public static String getVirtualizeJDBCUrl(JdbcDatabaseContainer mysql) {
        return mysql.getJdbcUrl() + JDBC_SSL_CONFIG;
    }

    public static Map<String, Object> getVirtualizeJDBCParameterMap(JdbcDatabaseContainer mysql, String query) {
        final String url = getVirtualizeJDBCUrl(mysql);
        return map(TYPE_KEY, JDBC_VALUE,
                URL_KEY, url,
                QUERY_KEY, query,
                DESC_KEY, JDBC_DESC,
                LABELS_KEY, JDBC_LABELS_AS_STRING);
    }


    record CsvTestResult(String name, String personName, String personAge, String hookNodeName, Map<String, Object> queryParams) { }

    static void getCsvCommonResult(GraphDatabaseService db, String csvUrl) {
        // TODO
        getCsvCommonResult(db, APOC_DV_ADD_QUERY, APOC_DV_LIST, APOC_DV_QUERY, csvUrl, db);
    }
    
    static void getCsvCommonResult(GraphDatabaseService dbRead, String dvAdd, String dvList, String dvQuery, String csvUrl, GraphDatabaseService dbWrite) {

        testCallEventually(dbWrite, dvAdd,
                map(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME, "name", CSV_NAME_VALUE, "map",
                map("type", "CSV",
                        "url", csvUrl, 
                        "query", QUERY_VALUE,
                        "desc", DESC_VALUE,
                        "labels", LABELS)),
//                APOC_DV_INSTALL_PARAMS,
                (row) -> assertCatalogContent(row, csvUrl), TIMEOUT);

        testCallEventually(dbWrite, dvList,
                (row) -> assertCatalogContent(row, csvUrl), TIMEOUT);

        testCallEventually(dbRead, dvQuery,
                map(NAME_KEY, CSV_NAME_VALUE, APOC_DV_QUERY_PARAMS_KEY, APOC_DV_QUERY_PARAMS, RELTYPE_KEY, RELTYPE_VALUE, CONFIG_KEY, CONFIG_VALUE),
//                APOC_DV_QUERY_AND_LINK_QUERY_PARAMS,
                (row) -> {
                    Node node = (Node) row.get(NODE_KEY);
                    assertEquals(PERSON_NAME, node.getProperty(NAME_KEY));
                    assertEquals(PERSON_AGE, node.getProperty(AGE_KEY));
                    assertEquals(List.of(Label.label(LABELS_VALUE)), node.getLabels());
                }, TIMEOUT);
        
//        final String name = "csv_vr";
//        final String url = getUrlFileName("test.csv").toString();
//        final String desc = "person's details";
//        final String query = "map.name = $name and map.age = $age";
//        List<String> labels = List.of("Person");
//        Map<String, Object> map = map("type", "CSV",
//                "url", url, "query", query,
//                "desc", desc,
//                "labels", labels);
//
//        String personName = "Rana";
//        String personAge = "11";
//        String hookNodeName = "node to test linking";

//        testCall(dbWrite, dvAdd,
//                APOC_DV_INSTALL_PARAMS,
////                map(DATABASE_NAME, dbRead.databaseName(), "name", CSV_NAME_VALUE, "map", MAP_VALUE),
//                (row) -> assertCatalogContent(row, CSV_TEST_FILE));
//
//        testCall(dbWrite, dvList,
//                (row) -> assertCatalogContent(row, CSV_TEST_FILE));
//
//        
//        testCall(dbRead, dvQuery,
//                APOC_DV_QUERY_AND_LINK_QUERY_PARAMS,
////                map(DATABASE_NAME, dbRead.databaseName(), "name", CSV_NAME_VALUE, "queryParams", queryParams, "config", map("header", true)),
//                (row) -> {
//                    Node node = (Node) row.get("node");
//                    assertEquals(PERSON_NAME, node.getProperty(NAME_KEY));
//                    assertEquals(PERSON_AGE, node.getProperty(AGE_KEY));
//                    assertEquals(List.of(Label.label(LABELS_VALUE)), node.getLabels());
//                });


        dbRead.executeTransactionally(CREATE_HOOK_QUERY, CREATE_HOOK_PARAMS);
//        "create (:Hook {name: $hookNodeName})", 
//                map(HOOK_NODE_NAME_KEY, HOOK_NODE_NAME_VALUE)
//        );
//        return new CsvTestResult(name, personName, personAge, hookNodeName, queryParams);
    }

    record VirtualizeJdbcResult(String name, List<Label> labels, String country, List<String> queryParams, String hookNodeName) {}

    static void getVirtualizeJdbcCommonResult(GraphDatabaseService db, JdbcDatabaseContainer mysql,
                                                              String dvAdd, String dvQueryWithParams, String dvQuery, GraphDatabaseService dbWrite) {

        String url = getVirtualizeJDBCUrl(mysql);
        testCall(dbWrite, dvAdd,
                map(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME, NAME_KEY, JDBC_NAME, "map", getVirtualizeJDBCParameterMap(mysql, VIRTUALIZE_JDBC_QUERY)),
                (row) -> assertDvQueryContent(row, url));

        testCallCount(db, dvQueryWithParams, map(
                        NAME_KEY, JDBC_NAME,
                        CONFIG_KEY, map(CREDENTIALS_KEY, getJdbcCredentials(mysql))),
                0
        );

        testCall(db, dvQuery,
                map(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, VIRTUALIZE_JDBC_APOC_PARAMS,
                        CONFIG_KEY, map(CREDENTIALS_KEY, getJdbcCredentials(mysql))),
                (row) -> {
                    Node node = (Node) row.get(NODE_KEY);
                    assertEquals(VIRTUALIZE_JDBC_COUNTRY, node.getProperty("Name"));
                    assertEquals(JDBC_LABELS, node.getLabels());
                });

        db.executeTransactionally(CREATE_HOOK_QUERY, CREATE_HOOK_PARAMS);
    }

    
    static void getVirtualizeJdbcWithParamsCommonResult(GraphDatabaseService db, JdbcDatabaseContainer mysql,
                                                        String dvAdd, GraphDatabaseService dbWrite) throws QueryExecutionException {
        String url = getVirtualizeJDBCUrl(mysql);
//        String name = "jdbc_vr";
//        String desc = "country details";
//        List<Label> labels = List.of(Label.label("Country"));
//        List<String> labelsAsString = List.of("Country");
//        final String query = "SELECT * FROM country WHERE Name = $name AND HeadOfState = $head_of_state AND Code2 = $CODE2";
//        final String url = mysql.getJdbcUrl() + "?useSSL=false";
//        Map<String, Object> map = map("type", "JDBC",
//                "url", url, "query", query,
//                "desc", desc,
//                "labels", labelsAsString);

        testCall(dbWrite, dvAdd,
                map(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,
                        NAME_KEY, JDBC_NAME, MAP_KEY, getVirtualizeJDBCParameterMap(mysql, VIRTUALIZE_JDBC_WITH_PARAMS_QUERY)),
                (row) -> assertDvCatalogAddOrInstall(row, url));

        testCallEmpty(db, APOC_DV_JDBC_WITH_PARAMS_QUERY,
                map(NAME_KEY, JDBC_NAME, 
                        CONFIG_KEY, map(CREDENTIALS_KEY, getJdbcCredentials(mysql))
                )
        );

//        String country = "Netherlands";
//        String code2 = "NL";
//        String headOfState = "Beatrix";
//        Map<String, Object> queryParams = map("name", country, "CODE2", code2, "head_of_state", headOfState);

        testCall(db, APOC_DV_QUERY,
                map(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, VIRTUALIZE_JDBC_QUERY_PARAMS,
                        CONFIG_KEY, map(CREDENTIALS_KEY, getJdbcCredentials(mysql))),
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(VIRTUALIZE_JDBC_COUNTRY, node.getProperty("Name"));
                    assertEquals(JDBC_LABELS, node.getLabels());
                });

        String hookNodeName = "node to test linking";

        db.executeTransactionally(CREATE_HOOK_QUERY, Map.of(HOOK_NODE_NAME_KEY, HOOK_NODE_NAME_VALUE));
//        db.executeTransactionally("create (:Hook {name: $hookNodeName})", map("hookNodeName", hookNodeName));
//        return new VirtualizeJdbcWithParameterResult(name, labels, country, queryParams, hookNodeName);
    }

}
