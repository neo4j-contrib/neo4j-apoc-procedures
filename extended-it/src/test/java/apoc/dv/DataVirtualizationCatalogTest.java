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
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;

import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.dv.DataVirtualizationCatalogTestUtil.*;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.getUrlFileName;
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
        final String url = getUrlFileName("test.csv").toString();
        getCsvCommonResult(db, url);

        final String relType = "LINKED_TO";
        testCall(db, APOC_DV_QUERY_AND_LINK_QUERY,
                APOC_DV_QUERY_AND_LINK_QUERY_PARAMS,
//                Map.of("name", result.name(), "queryParams", result.queryParams(), "relType", relType, "config", Map.of("header", true)),
                DataVirtualizationCatalogTestUtil::assertVirtualizeCSVQueryAndLinkContent);
//                (row) -> {
//                    Path path = (Path) row.get("path");
//                    Node node = path.endNode();
//                    assertEquals(result.personName(), node.getProperty("name"));
//                    assertEquals(result.personAge(), node.getProperty("age"));
//                    assertEquals(List.of(Label.label("Person")), node.getLabels());
//
//                    Node hook = path.startNode();
//                    assertEquals(result.hookNodeName(), hook.getProperty("name"));
//                    assertEquals(List.of(Label.label("Hook")), hook.getLabels());
//
//                    Relationship relationship = path.lastRelationship();
//                    assertEquals(hook, relationship.getStartNode());
//                    assertEquals(node, relationship.getEndNode());
//                    assertEquals(relType, relationship.getType().name());
//                });

    }

    @Test
    public void testVirtualizeCSVWithCustomDirectionIN() {
        final String url = getUrlFileName("test.csv").toString();
        getCsvCommonResult(db, url);

        Map<String, Object> config = withDirectionIn(CONFIG_VALUE);
        testCall(db, APOC_DV_QUERY_AND_LINK_QUERY,
                map(NAME_KEY, CSV_NAME_VALUE, APOC_DV_QUERY_PARAMS_KEY, APOC_DV_QUERY_PARAMS, RELTYPE_KEY, RELTYPE_VALUE, CONFIG_KEY, config),
                DataVirtualizationCatalogTestUtil::assertVirtualizeCSVQueryAndLinkContentDirectionIN);
//                (row) -> {
//                    Path path = (Path) row.get("path");
//                    Node hook = path.endNode();
//                    assertEquals(result.hookNodeName(), hook.getProperty("name"));
//                    assertEquals(List.of(Label.label("Hook")), hook.getLabels());
//                    Node node = path.startNode();
//
//                    assertEquals(result.personName(), node.getProperty("name"));
//                    assertEquals(result.personAge(), node.getProperty("age"));
//                    assertEquals(List.of(Label.label("Person")), node.getLabels());
//
//                    Relationship relationship = path.lastRelationship();
//                    assertEquals(node, relationship.getStartNode());
//                    assertEquals(hook, relationship.getEndNode());
//                    assertEquals(relType, relationship.getType().name());
//                });

    }

    @Test
    public void testVirtualizeJDBC() {
        getVirtualizeJdbcCommonResult(db, mysql, APOC_DV_ADD_QUERY, APOC_DV_QUERY_WITH_PARAM, APOC_DV_QUERY, db);

        testCall(db, APOC_DV_QUERY_AND_LINK_QUERY,
                Map.of(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, VIRTUALIZE_JDBC_APOC_PARAMS, RELTYPE_KEY, VIRTUALIZE_JDBC_WITH_PARAMS_RELTYPE,
                        CONFIG_KEY, getJdbcCredentials(mysql)),
                DataVirtualizationCatalogTestUtil::assertDvQueryAndLinkContent);
//
//        final String relType = "LINKED_TO_NEW";
//        testCall(db, "MATCH (hook:Hook) WITH hook " +
//                     "CALL apoc.dv.queryAndLink(hook, $relType, $name, $queryParams, $config) yield path " +
//                     "RETURN path ",
//                Map.of("name", PERSON_NAME, "queryParams", APOC_DV_QUERY_WITH_PARAM, "relType", relType,
//                        "config", getJdbcCredentials(mysql)),
//                DataVirtualizationCatalogTestUtil::assertDvQueryAndLinkContent);
//                (row) -> {
//                    Path path = (Path) row.get("path");
//                    Node node = path.endNode();
//                    assertEquals(result.country(), node.getProperty("Name"));
//                    assertEquals(result.labels(), node.getLabels());
//
//                    Node hook = path.startNode();
//                    assertEquals(result.hookNodeName(), hook.getProperty("name"));
//                    assertEquals(List.of(Label.label("Hook")), hook.getLabels());
//
//                    Relationship relationship = path.lastRelationship();
//                    assertEquals(hook, relationship.getStartNode());
//                    assertEquals(node, relationship.getEndNode());
//                    assertEquals(relType, relationship.getType().name());
//                });
    }

    @Test
    public void testVirtualizeJDBCWithCustomDirectionIN() {
        getVirtualizeJdbcCommonResult(db, mysql, APOC_DV_ADD_QUERY, APOC_DV_QUERY_WITH_PARAM, APOC_DV_QUERY, db);

        Map<String, Object> config = getJdbcCredentials(mysql);
        withDirectionIn(config);

        testCall(db, APOC_DV_QUERY_AND_LINK_QUERY,
                Map.of(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, VIRTUALIZE_JDBC_APOC_PARAMS, RELTYPE_KEY, VIRTUALIZE_JDBC_WITH_PARAMS_RELTYPE,
                        CONFIG_KEY, config),
                DataVirtualizationCatalogTestUtil::assertDvQueryAndLinkContentDirectionIN);
        
//        final String relType = "LINKED_TO_NEW";
//        testCall(db, "MATCH (hook:Hook) WITH hook " +
//                     "CALL apoc.dv.queryAndLink(hook, $relType, $name, $queryParams, $config) yield path " +
//                     "RETURN path ",
//                Map.of("name", PERSON_NAME, "queryParams", APOC_DV_QUERY_WITH_PARAM, "relType", RELTYPE_VALUE,
//                        "config", config),
//                DataVirtualizationCatalogTestUtil::assertDvQueryAndLinkContentDirectionIn);
//                (row) -> {
//                    Path path = (Path) row.get("path");
//                    Node hook = path.endNode();
//                    assertEquals(result.hookNodeName(), hook.getProperty("name"));
//                    assertEquals(List.of(Label.label("Hook")), hook.getLabels());
//
//                    Node node = path.startNode();
//                    assertEquals(result.country(), node.getProperty("Name"));
//                    assertEquals(result.labels(), node.getLabels());
//
//                    Relationship relationship = path.lastRelationship();
//                    assertEquals(node, relationship.getStartNode());
//                    assertEquals(hook, relationship.getEndNode());
//                    assertEquals(relType, relationship.getType().name());
//                });
    }

    @Test
    public void testVirtualizeJDBCWithParameterMap() {
        getVirtualizeJdbcWithParamsCommonResult(db, mysql, APOC_DV_ADD_QUERY, db);

        testCall(db, APOC_DV_QUERY_AND_LINK_QUERY,
                Map.of(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, VIRTUALIZE_JDBC_QUERY_PARAMS, RELTYPE_KEY, VIRTUALIZE_JDBC_WITH_PARAMS_RELTYPE,
                        CONFIG_KEY, getJdbcCredentials(mysql)),
                DataVirtualizationCatalogTestUtil::assertDvQueryAndLinkContent);
//        testCall(db, APOC_DV_QUERY_AND_LINK_QUERY,
//                Map.of(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, VIRTUALIZE_JDBC_APOC_PARAMS, RELTYPE_KEY, VIRTUALIZE_JDBC_QUERY_PARAMS,
//                        CONFIG_KEY, Map.of(
//                                "credentials", Map.of("user", mysql.getUsername(), "password", mysql.getPassword())
//                        )),
                
//                Map.of("name", result.name(), "queryParams", result.queryParams(), "relType", relType,
//                        "config", Map.of("credentials", Map.of("user", mysql.getUsername(), "password", mysql.getPassword()))),
//                DataVirtualizationCatalogTestUtil::assertDvQueryAndLinkContent);
//                (row) -> {
//                    Path path = (Path) row.get("path");
//                    Node node = path.endNode();
//                    assertEquals(result.country(), node.getProperty("Name"));
//                    assertEquals(result.labels(), node.getLabels());
//
//                    Node hook = path.startNode();
//                    assertEquals(result.hookNodeName(), hook.getProperty("name"));
//                    assertEquals(List.of(Label.label("Hook")), hook.getLabels());
//
//                    Relationship relationship = path.lastRelationship();
//                    assertEquals(hook, relationship.getStartNode());
//                    assertEquals(node, relationship.getEndNode());
//                    assertEquals(relType, relationship.getType().name());
//                });
    }


    @Test
    public void testVirtualizeJDBCWithParameterMapAndDirectionIN() {
        getVirtualizeJdbcWithParamsCommonResult(db, mysql, APOC_DV_ADD_QUERY, db);

        Map<String, Object> config = map("credentials", getJdbcCredentials(mysql));
        withDirectionIn(config);
        testCall(db, APOC_DV_QUERY_AND_LINK_QUERY,
                Map.of(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, VIRTUALIZE_JDBC_QUERY_PARAMS, RELTYPE_KEY, VIRTUALIZE_JDBC_WITH_PARAMS_RELTYPE,
                        CONFIG_KEY, config),
                DataVirtualizationCatalogTestUtil::assertDvQueryAndLinkContentDirectionIN);
//
//        testCall(db, APOC_DV_QUERY_AND_LINK_QUERY,
//                Map.of(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, VIRTUALIZE_JDBC_APOC_PARAMS, RELTYPE_KEY, VIRTUALIZE_JDBC_QUERY_PARAMS,
//                        CONFIG_KEY, Map.of(
//                                "direction", "IN",
//                                "credentials", Map.of("user", mysql.getUsername(), "password", mysql.getPassword())
//                        )),
////                Map.of("name", result.name(), "queryParams", result.queryParams(), "relType", relType,
////                        "config", Map.of("credentials", Map.of("user", mysql.getUsername(), "password", mysql.getPassword()),
////                                "direction", "IN"
////                        )),
//                DataVirtualizationCatalogTestUtil::assertDvQueryAndLinkContentDirectionIN);
//                (row) -> {
//                    Path path = (Path) row.get("path");
//                    Node hook = path.endNode();
//                    assertEquals(result.hookNodeName(), hook.getProperty("name"));
//                    assertEquals(List.of(Label.label("Hook")), hook.getLabels());
//
//                    Node node = path.startNode();
//                    assertEquals(result.country(), node.getProperty("Name"));
//                    assertEquals(result.labels(), node.getLabels());
//
//                    Relationship relationship = path.lastRelationship();
//                    assertEquals(node, relationship.getStartNode());
//                    assertEquals(hook, relationship.getEndNode());
//                    assertEquals(relType, relationship.getType().name());
//                });
    }

    @Test
    public void testRemove() {
        db.executeTransactionally(APOC_DV_ADD_QUERY,
                Map.of("name", JDBC_NAME, "map", getVirtualizeJDBCParameterMap(mysql, JDBC_SELECT_QUERY)));

        testCallEmpty(db, "CALL apoc.dv.catalog.remove($name)", Map.of("name", JDBC_NAME));
    }

    @Test
    public void testNameAsKey() {
        Map<String, Object> params = Map.of(
                NAME_KEY, JDBC_NAME, "map", getVirtualizeJDBCParameterMap(mysql, JDBC_SELECT_QUERY)
        );

        db.executeTransactionally(APOC_DV_ADD_QUERY, params);
        db.executeTransactionally(APOC_DV_ADD_QUERY, params);
        testResult(db, "CALL apoc.dv.catalog.list()",
                Map.of(),
                (result) -> assertEquals(1, result.stream().count()));
    }

    @Test
    public void testJDBCQueryWithMixedParamsTypes() {
        try {
            db.executeTransactionally(APOC_DV_ADD_QUERY,
                    Map.of("name", JDBC_NAME, "map", getVirtualizeJDBCParameterMap(mysql, JDBC_SELECT_QUERY_WITH_PARAM)));
            Assert.fail("Exception is expected");
        } catch (Exception e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            assertTrue(rootCause instanceof IllegalArgumentException);
            assertEquals("The query is mixing parameters with `$` and `?` please use just one notation", rootCause.getMessage());
        }
    }

    @Test
    public void testVirtualizeJDBCWithDifferentParameterMap() {
        final String url = mysql.getJdbcUrl() + "?useSSL=false";
        testCall(db, APOC_DV_ADD_QUERY,
                Map.of("name", JDBC_NAME, "map", getVirtualizeJDBCParameterMap(mysql, VIRTUALIZE_JDBC_WITH_PARAMS_QUERY)),
                (row) -> assertDvCatalogAddOrInstall(row, url));

        String country = "Netherlands";
        String code2 = "NL";
        String headOfState = "Beatrix";
        Map<String, Object> queryParams = Map.of("foo", country, "bar", code2, "baz", headOfState);

        try {
            db.executeTransactionally(APOC_DV_QUERY,
                    Map.of(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, queryParams,
                            CONFIG_KEY, getJdbcCredentials(mysql)),
                    Result::resultAsString);
            Assert.fail("Exception is expected");
        } catch (Exception e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            assertTrue(rootCause instanceof IllegalArgumentException);
            final List<String> actualParams = queryParams.keySet().stream()
                    .map(s -> "$" + s)
                    .sorted()
                    .toList();
            assertEquals(String.format("Expected query parameters are %s, actual are %s", EXPECTED_LIST_SORTED, actualParams), rootCause.getMessage());
        }
    }
  
}
