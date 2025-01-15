package apoc.full.it.dv;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.full.it.dv.DataVirtualizationCatalogTestUtil.*;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallCount;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import apoc.create.Create;
import apoc.dv.DataVirtualizationCatalog;
import apoc.dv.DataVirtualizationCatalogNewProcedures;
import apoc.load.Jdbc;
import apoc.load.LoadCsv;
import apoc.util.TestUtil;
import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;

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
        databaseManagementService =
                new TestDatabaseManagementServiceBuilder(storeDir.getRoot().toPath()).build();
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
    }

    @AfterClass
    public static void tearDownContainer() {
        mysql.stop();
    }

    @Test
    public void testVirtualizeCSV() {
        getVirtualizeCSVCommonResult(db, APOC_DV_INSTALL_QUERY, APOC_DV_SHOW_QUERY, CSV_TEST_FILE, sysDb);

        testCall(
                db,
                APOC_DV_QUERY_AND_LINK_QUERY,
                map(
                        NAME_KEY,
                        CSV_NAME_VALUE,
                        APOC_DV_QUERY_PARAMS_KEY,
                        APOC_DV_QUERY_PARAMS,
                        RELTYPE_KEY,
                        RELTYPE_VALUE,
                        CONFIG_KEY,
                        CONFIG_VALUE),
                DataVirtualizationCatalogTestUtil::assertVirtualizeCSVQueryAndLinkContent);
    }

    @Test
    public void testVirtualizeCSVWithCustomDirectionIN() {
        getVirtualizeCSVCommonResult(db, APOC_DV_INSTALL_QUERY, APOC_DV_SHOW_QUERY, CSV_TEST_FILE, sysDb);

        Map<String, Object> config = new HashMap<>(CONFIG_VALUE);
        config.put(DIRECTION_CONF_KEY, DataVirtualizationCatalog.Direction.IN.name());
        testCall(
                db,
                APOC_DV_QUERY_AND_LINK_QUERY,
                map(
                        NAME_KEY,
                        CSV_NAME_VALUE,
                        APOC_DV_QUERY_PARAMS_KEY,
                        APOC_DV_QUERY_PARAMS,
                        RELTYPE_KEY,
                        RELTYPE_VALUE,
                        CONFIG_KEY,
                        config),
                DataVirtualizationCatalogTestUtil::assertVirtualizeCSVQueryAndLinkContentDirectionIN);
    }

    @Test
    public void testVirtualizeJDBC() {

        getVirtualizeJDBCCommonResult(db, mysql, APOC_DV_INSTALL_QUERY, sysDb);

        testCall(
                db,
                APOC_DV_QUERY_AND_LINK_QUERY,
                map(
                        NAME_KEY,
                        JDBC_NAME,
                        APOC_DV_QUERY_PARAMS_KEY,
                        VIRTUALIZE_JDBC_APOC_PARAMS,
                        RELTYPE_KEY,
                        VIRTUALIZE_JDBC_WITH_PARAMS_RELTYPE,
                        CONFIG_KEY,
                        map(CREDENTIALS_KEY, getJdbcCredentials(mysql))),
                DataVirtualizationCatalogTestUtil::assertVirtualizeJDBCQueryAndLinkContent);
    }

    @Test
    public void testVirtualizeJDBCWithCustomDirectionIN() {

        getVirtualizeJDBCCommonResult(db, mysql, APOC_DV_INSTALL_QUERY, sysDb);

        testCall(
                db,
                APOC_DV_QUERY_AND_LINK_QUERY,
                map(
                        NAME_KEY,
                        JDBC_NAME,
                        APOC_DV_QUERY_PARAMS_KEY,
                        VIRTUALIZE_JDBC_APOC_PARAMS,
                        RELTYPE_KEY,
                        VIRTUALIZE_JDBC_WITH_PARAMS_RELTYPE,
                        CONFIG_KEY,
                        map(
                                DIRECTION_CONF_KEY, DataVirtualizationCatalog.Direction.IN.name(),
                                CREDENTIALS_KEY, getJdbcCredentials(mysql))),
                DataVirtualizationCatalogTestUtil::assertVirtualizeJDBCQueryAndLinkContentDirectionIN);
    }

    @Test
    public void testVirtualizeJDBCWithParameterMap() {

        getVirtualizeJDBCWithParamsCommonResult(db, mysql, APOC_DV_INSTALL_QUERY, sysDb);

        testCall(
                db,
                APOC_DV_QUERY_AND_LINK_QUERY,
                map(
                        NAME_KEY,
                        JDBC_NAME,
                        APOC_DV_QUERY_PARAMS_KEY,
                        VIRTUALIZE_JDBC_QUERY_PARAMS,
                        RELTYPE_KEY,
                        VIRTUALIZE_JDBC_WITH_PARAMS_RELTYPE,
                        CONFIG_KEY,
                        map(CREDENTIALS_KEY, getJdbcCredentials(mysql))),
                DataVirtualizationCatalogTestUtil::assertVirtualizeJDBCQueryAndLinkContent);
    }

    @Test
    public void testVirtualizeJDBCWithParameterMapAndDirectionIN() {

        getVirtualizeJDBCWithParamsCommonResult(db, mysql, APOC_DV_INSTALL_QUERY, sysDb);

        testCall(
                db,
                APOC_DV_QUERY_AND_LINK_QUERY,
                map(
                        NAME_KEY,
                        JDBC_NAME,
                        APOC_DV_QUERY_PARAMS_KEY,
                        VIRTUALIZE_JDBC_QUERY_PARAMS,
                        RELTYPE_KEY,
                        VIRTUALIZE_JDBC_WITH_PARAMS_RELTYPE,
                        CONFIG_KEY,
                        map(
                                DIRECTION_CONF_KEY, DataVirtualizationCatalog.Direction.IN.name(),
                                CREDENTIALS_KEY, getJdbcCredentials(mysql))),
                DataVirtualizationCatalogTestUtil::assertVirtualizeJDBCQueryAndLinkContentDirectionIN);
    }

    @Test
    public void testRemove() {
        sysDb.executeTransactionally(
                APOC_DV_INSTALL_QUERY,
                map(
                        DATABASE_NAME,
                        GraphDatabaseSettings.DEFAULT_DATABASE_NAME,
                        NAME_KEY,
                        JDBC_NAME,
                        "map",
                        getVirtualizeJDBCParameterMap(mysql, JDBC_SELECT_QUERY)));

        testCallCount(
                sysDb,
                APOC_DV_DROP_QUERY,
                map(DATABASE_NAME, GraphDatabaseSettings.DEFAULT_DATABASE_NAME, NAME_KEY, JDBC_NAME),
                0);
    }

    @Test
    public void testNameAsKey() {
        Map<String, Object> params = map(
                DATABASE_NAME,
                GraphDatabaseSettings.DEFAULT_DATABASE_NAME,
                NAME_KEY,
                JDBC_NAME,
                "map",
                getVirtualizeJDBCParameterMap(mysql, JDBC_SELECT_QUERY));

        sysDb.executeTransactionally(APOC_DV_INSTALL_QUERY, params);
        sysDb.executeTransactionally(APOC_DV_INSTALL_QUERY, params);
        testResult(
                sysDb,
                APOC_DV_SHOW_QUERY,
                (result) -> assertEquals(1, result.stream().count()));
    }

    @Test
    public void testJDBCQueryWithMixedParamsTypes() {
        try {
            sysDb.executeTransactionally(
                    APOC_DV_INSTALL_QUERY,
                    map(
                            DATABASE_NAME,
                            GraphDatabaseSettings.DEFAULT_DATABASE_NAME,
                            NAME_KEY,
                            JDBC_NAME,
                            "map",
                            getVirtualizeJDBCParameterMap(mysql, JDBC_SELECT_QUERY_WITH_PARAM)));
            Assert.fail("Exception is expected");
        } catch (Exception e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            assertTrue(rootCause instanceof IllegalArgumentException);
            assertEquals(
                    "The query is mixing parameters with `$` and `?` please use just one notation",
                    rootCause.getMessage());
        }
    }

    @Test
    public void testVirtualizeJDBCWithDifferentParameterMap() {
        final String url = getVirtualizeJDBCUrl(mysql);
        final List<String> expectedParams = List.of("$name", "$head_of_state", "$CODE2");
        final List<String> sortedExpectedParams =
                expectedParams.stream().sorted().toList();
        testCall(
                sysDb,
                APOC_DV_INSTALL_QUERY,
                map(
                        DATABASE_NAME,
                        GraphDatabaseSettings.DEFAULT_DATABASE_NAME,
                        NAME_KEY,
                        JDBC_NAME,
                        "map",
                        getVirtualizeJDBCParameterMap(mysql, VIRTUALIZE_JDBC_WITH_PARAMS_QUERY)),
                (row) -> assertDvCatalogAddOrInstall(row, url));

        try {
            db.executeTransactionally(
                    APOC_DV_QUERY,
                    map(
                            NAME_KEY,
                            JDBC_NAME,
                            APOC_DV_QUERY_PARAMS_KEY,
                            VIRTUALIZE_JDBC_QUERY_WRONG_PARAMS,
                            CONFIG_KEY,
                            map(CREDENTIALS_KEY, getJdbcCredentials(mysql))),
                    Result::resultAsString);
            Assert.fail("Exception is expected");
        } catch (Exception e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            assertTrue(rootCause instanceof IllegalArgumentException);
            final List<String> actualParams = VIRTUALIZE_JDBC_QUERY_WRONG_PARAMS.keySet().stream()
                    .map(s -> "$" + s)
                    .sorted()
                    .toList();
            assertEquals(
                    String.format(
                            "Expected query parameters are %s, actual are %s", sortedExpectedParams, actualParams),
                    rootCause.getMessage());
        }
    }
}
