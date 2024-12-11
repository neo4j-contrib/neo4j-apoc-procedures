package apoc.dv;

import apoc.create.Create;
import apoc.load.Jdbc;
import apoc.load.LoadCsv;
import apoc.util.TestUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.custom.CypherProcedureTestUtil.startDbWithCustomApocConfigs;
import static apoc.dv.DataVirtualizationCatalog.DIRECTION_CONF_KEY;
import static apoc.dv.DataVirtualizationCatalogTestUtil.*;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallCount;

import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DataVirtualizationCatalogNewProcedureTest {
    private static final String DATABASE_NAME = "databaseName";
    private static GraphDatabaseService sysDb;
    private static GraphDatabaseService db;
    private static DatabaseManagementService databaseManagementService;
    
    public static JdbcDatabaseContainer mysql;

    @Rule
    public TemporaryFolder storeDir = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        databaseManagementService = startDbWithCustomApocConfigs(storeDir);
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        sysDb = databaseManagementService.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
        FileUtils.copyFile(new File(new URI(FILE_URL).toURL().getPath()), new File(storeDir.getRoot(), CSV_TEST_FILE));
        TestUtil.registerProcedure(sysDb, DataVirtualizationCatalogNewProcedures.class);
        TestUtil.registerProcedure(db, DataVirtualizationCatalog.class, Jdbc.class, LoadCsv.class, Create.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
    }

    @BeforeClass
    public static void setUpContainer() {
        mysql = new MySQLContainer().withInitScript("init_mysql.sql");
        mysql.start();
        
//        url = getVirtualizeJDBCUrl(mysql);
    }

    @AfterClass
    public static void tearDownContainer() {
        mysql.stop();
    }

    @Test
    public void testVirtualizeCSV() {
        getCsvCommonResult(db,
                APOC_DV_INSTALL_QUERY, APOC_DV_SHOW_QUERY, APOC_DV_QUERY, CSV_TEST_FILE, sysDb);

        testCall(db, APOC_DV_QUERY_AND_LINK_QUERY, APOC_DV_QUERY_AND_LINK_QUERY_PARAMS,
                DataVirtualizationCatalogTestUtil::assertVirtualizeCSVQueryAndLinkContent);

    }

    @Test
    public void testVirtualizeCSVWithCustomDirectionIN() {
        getCsvCommonResult(db,
                APOC_DV_INSTALL_QUERY, APOC_DV_SHOW_QUERY, APOC_DV_QUERY, CSV_TEST_FILE, sysDb);

        Map<String, Object> params = APOC_DV_QUERY_AND_LINK_QUERY_PARAMS;
        Map map = (Map) params.get(CONFIG_KEY);
        map.put("direction", "IN");
        testCall(db, APOC_DV_QUERY_AND_LINK_QUERY, params,
                DataVirtualizationCatalogTestUtil::assertVirtualizeCSVQueryAndLinkContentDirectionIN);
    }

    @Test
    public void testVirtualizeJDBC() {

        getVirtualizeJdbcCommonResult(db, mysql,
                APOC_DV_INSTALL_QUERY, APOC_DV_QUERY_WITH_PARAM, APOC_DV_QUERY, sysDb);

        testCall(db, APOC_DV_QUERY_AND_LINK_QUERY,
                map(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, VIRTUALIZE_JDBC_APOC_PARAMS, RELTYPE_KEY, VIRTUALIZE_JDBC_WITH_PARAMS_RELTYPE,
                        CONFIG_KEY, map(CREDENTIALS_KEY, getJdbcCredentials(mysql))),
                DataVirtualizationCatalogTestUtil::assertDvQueryAndLinkContent);
    }

    @Test
    public void testVirtualizeJDBCWithCustomDirectionIN() {

        getVirtualizeJdbcCommonResult(db, mysql,
                APOC_DV_INSTALL_QUERY, APOC_DV_QUERY_WITH_PARAM, APOC_DV_QUERY, sysDb);

        testCall(db, APOC_DV_QUERY_AND_LINK_QUERY,
                map(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, VIRTUALIZE_JDBC_APOC_PARAMS, RELTYPE_KEY, VIRTUALIZE_JDBC_WITH_PARAMS_RELTYPE,
                        CONFIG_KEY, map(
                                DIRECTION_CONF_KEY, DataVirtualizationCatalog.Direction.IN.name(),
                                CREDENTIALS_KEY, getJdbcCredentials(mysql)
                        )),
                DataVirtualizationCatalogTestUtil::assertDvQueryAndLinkContentDirectionIN);
    }
    
    @Test
    public void testVirtualizeJDBCWithParameterMap() {

        getVirtualizeJdbcWithParamsCommonResult(db, mysql, APOC_DV_INSTALL_QUERY, sysDb);

        testCall(db, APOC_DV_QUERY_AND_LINK_QUERY,
                map(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, VIRTUALIZE_JDBC_QUERY_PARAMS, RELTYPE_KEY, VIRTUALIZE_JDBC_WITH_PARAMS_RELTYPE,
                        CONFIG_KEY, map(
                                CREDENTIALS_KEY, getJdbcCredentials(mysql)
                        )),
                DataVirtualizationCatalogTestUtil::assertDvQueryAndLinkContent);
//
//        testCall(sysDb, APOC_DV_INSTALL_QUERY,
//                map(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,NAME_KEY, JDBC_NAME,
//                        "map", getVirtualizeJDBCParameterMap(mysql, VIRTUALIZE_JDBC_WITH_PARAMS_QUERY)),
//                (row) -> assertDvCatalogAddOrInstall(row, url));
//
//        testCallEmpty(db, APOC_DV_JDBC_WITH_PARAMS_QUERY,
//                map(NAME_KEY, JDBC_NAME, CONFIG_KEY, getJdbcCredentials(mysql)));
//
//
//        testCall(db, APOC_DV_QUERY,
//                map(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, VIRTUALIZE_JDBC_QUERY_PARAMS,
//                        CONFIG_KEY, getJdbcCredentials(mysql)),
//                (row) -> {
//                    Node node = (Node) row.get(NODE_KEY);
//                    assertEquals(VIRTUALIZE_JDBC_COUNTRY, node.getProperty("Name"));
//                    assertEquals(JDBC_LABELS, node.getLabels());
//                });
//
//        db.executeTransactionally(CREATE_HOOK_QUERY, map(HOOK_NODE_NAME_KEY, HOOK_NODE_NAME_VALUE));
//
//        testCall(db, APOC_DV_QUERY_AND_LINK_QUERY,
//                map(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, VIRTUALIZE_JDBC_QUERY_PARAMS, RELTYPE_KEY, VIRTUALIZE_JDBC_WITH_PARAMS_RELTYPE,
//                        CONFIG_KEY, getJdbcCredentials(mysql)),
//                DataVirtualizationCatalogTestUtil::assertDvQueryAndLinkContent);
    }

    @Test
    public void testVirtualizeJDBCWithParameterMapAndDirectionIN() {

        getVirtualizeJdbcWithParamsCommonResult(db, mysql, APOC_DV_INSTALL_QUERY, sysDb);

        testCall(db, APOC_DV_QUERY_AND_LINK_QUERY,
                map(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, VIRTUALIZE_JDBC_QUERY_PARAMS, RELTYPE_KEY, VIRTUALIZE_JDBC_WITH_PARAMS_RELTYPE,
                        CONFIG_KEY, map(
                                DIRECTION_CONF_KEY, DataVirtualizationCatalog.Direction.IN.name(),
                                CREDENTIALS_KEY, getJdbcCredentials(mysql)
                        )),
                DataVirtualizationCatalogTestUtil::assertDvQueryAndLinkContentDirectionIN);
    }

    @Test
    public void testRemove() {
        sysDb.executeTransactionally(APOC_DV_INSTALL_QUERY,
                map(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,NAME_KEY, JDBC_NAME, "map", getVirtualizeJDBCParameterMap(mysql, JDBC_SELECT_QUERY)));

        testCallCount(sysDb, APOC_DV_DROP_QUERY, map(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,NAME_KEY, JDBC_NAME), 0);
    }

    @Test
    public void testNameAsKey() {
        Map<String, Object> params = map(
                DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,
                NAME_KEY, JDBC_NAME, "map", getVirtualizeJDBCParameterMap(mysql, JDBC_SELECT_QUERY)
        );

        sysDb.executeTransactionally(APOC_DV_INSTALL_QUERY, params);
        sysDb.executeTransactionally(APOC_DV_INSTALL_QUERY, params);
        testResult(sysDb, APOC_DV_SHOW_QUERY,
                (result) -> assertEquals(1, result.stream().count()));
    }

    @Test
    public void testJDBCQueryWithMixedParamsTypes() {
        try {
            sysDb.executeTransactionally(APOC_DV_INSTALL_QUERY,
                    map(
                            DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,NAME_KEY, JDBC_NAME,
                            "map", getVirtualizeJDBCParameterMap(mysql, JDBC_SELECT_QUERY_WITH_PARAM)
                    )
            );
            Assert.fail("Exception is expected");
        } catch (Exception e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            assertTrue(rootCause instanceof IllegalArgumentException);
            assertEquals("The query is mixing parameters with `$` and `?` please use just one notation", rootCause.getMessage());
        }
    }

    @Test
    public void testVirtualizeJDBCWithDifferentParameterMap() {
        final String url = getVirtualizeJDBCUrl(mysql);
        final List<String> expectedParams = List.of("$name", "$head_of_state", "$CODE2");
        final List<String> sortedExpectedParams = expectedParams.stream()
                .sorted()
                .toList();
        testCall(sysDb, APOC_DV_INSTALL_QUERY,
                map(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME,NAME_KEY, JDBC_NAME, "map", getVirtualizeJDBCParameterMap(mysql, VIRTUALIZE_JDBC_WITH_PARAMS_QUERY)),
                (row) -> assertDvCatalogAddOrInstall(row, url));

        try {
            db.executeTransactionally(APOC_DV_QUERY,
                    map(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, VIRTUALIZE_JDBC_QUERY_WRONG_PARAMS,
                            CONFIG_KEY, map("credentials", getJdbcCredentials(mysql))),
                    Result::resultAsString);
            Assert.fail("Exception is expected");
        } catch (Exception e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            assertTrue(rootCause instanceof IllegalArgumentException);
            final List<String> actualParams = VIRTUALIZE_JDBC_QUERY_WRONG_PARAMS.keySet().stream()
                    .map(s -> "$" + s)
                    .sorted()
                    .toList();
            assertEquals(String.format("Expected query parameters are %s, actual are %s", sortedExpectedParams, actualParams), rootCause.getMessage());
        }
    }
}
