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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.dv.DataVirtualizationCatalog.DIRECTION_CONF_KEY;
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
        getVirtualizeCSVCommonResult(db, APOC_DV_ADD_QUERY, APOC_DV_LIST, url, db);

        testCall(db, APOC_DV_QUERY_AND_LINK_QUERY,
                map(NAME_KEY, CSV_NAME_VALUE, APOC_DV_QUERY_PARAMS_KEY, APOC_DV_QUERY_PARAMS, RELTYPE_KEY, RELTYPE_VALUE, CONFIG_KEY, CONFIG_VALUE),
                DataVirtualizationCatalogTestUtil::assertVirtualizeCSVQueryAndLinkContent);

    }

    @Test
    public void testVirtualizeCSVWithCustomDirectionIN() {
        final String url = getUrlFileName("test.csv").toString();
        getVirtualizeCSVCommonResult(db, APOC_DV_ADD_QUERY, APOC_DV_LIST, url, db);

        Map<String, Object> config = new HashMap<>(CONFIG_VALUE);
        config.put(DIRECTION_CONF_KEY, DataVirtualizationCatalog.Direction.IN.name());
        
        testCall(db, APOC_DV_QUERY_AND_LINK_QUERY,
                map(NAME_KEY, CSV_NAME_VALUE, APOC_DV_QUERY_PARAMS_KEY, APOC_DV_QUERY_PARAMS, RELTYPE_KEY, RELTYPE_VALUE, CONFIG_KEY, config),
                DataVirtualizationCatalogTestUtil::assertVirtualizeCSVQueryAndLinkContentDirectionIN);
    }

    @Test
    public void testVirtualizeJDBC() {
        getVirtualizeJDBCCommonResult(db, mysql, APOC_DV_ADD_QUERY, db);

        testCall(db, APOC_DV_QUERY_AND_LINK_QUERY,
                map(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, VIRTUALIZE_JDBC_APOC_PARAMS, RELTYPE_KEY, VIRTUALIZE_JDBC_WITH_PARAMS_RELTYPE,
                        CONFIG_KEY, map(CREDENTIALS_KEY, getJdbcCredentials(mysql))),
                DataVirtualizationCatalogTestUtil::assertVirtualizeJDBCQueryAndLinkContent);
    }

    @Test
    public void testVirtualizeJDBCWithCustomDirectionIN() {
        getVirtualizeJDBCCommonResult(db, mysql, APOC_DV_ADD_QUERY, db);
        

        testCall(db, APOC_DV_QUERY_AND_LINK_QUERY,
                map(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, VIRTUALIZE_JDBC_APOC_PARAMS, RELTYPE_KEY, VIRTUALIZE_JDBC_WITH_PARAMS_RELTYPE,
                        CONFIG_KEY, map(
                                DIRECTION_CONF_KEY, DataVirtualizationCatalog.Direction.IN.name(),
                                CREDENTIALS_KEY, getJdbcCredentials(mysql)
                        )),
                DataVirtualizationCatalogTestUtil::assertVirtualizeJDBCQueryAndLinkContentDirectionIN);
    }

    @Test
    public void testVirtualizeJDBCWithParameterMap() {
        getVirtualizeJDBCWithParamsCommonResult(db, mysql, APOC_DV_ADD_QUERY, db);

        testCall(db, APOC_DV_QUERY_AND_LINK_QUERY,
                map(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, VIRTUALIZE_JDBC_QUERY_PARAMS, RELTYPE_KEY, VIRTUALIZE_JDBC_WITH_PARAMS_RELTYPE,
                        CONFIG_KEY, map(CREDENTIALS_KEY, getJdbcCredentials(mysql))
                ),
                DataVirtualizationCatalogTestUtil::assertVirtualizeJDBCQueryAndLinkContent);
    }

    @Test
    public void testVirtualizeJDBCWithParameterMapAndDirectionIN() {
        getVirtualizeJDBCWithParamsCommonResult(db, mysql, APOC_DV_ADD_QUERY, db);

        testCall(db, APOC_DV_QUERY_AND_LINK_QUERY,
                map(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, VIRTUALIZE_JDBC_QUERY_PARAMS, RELTYPE_KEY, VIRTUALIZE_JDBC_WITH_PARAMS_RELTYPE,
                        CONFIG_KEY, map(
                                DIRECTION_CONF_KEY, DataVirtualizationCatalog.Direction.IN.name(),
                                CREDENTIALS_KEY, getJdbcCredentials(mysql)
                        )),
                DataVirtualizationCatalogTestUtil::assertVirtualizeJDBCQueryAndLinkContentDirectionIN);
    }

    @Test
    public void testRemove() {
        db.executeTransactionally(APOC_DV_ADD_QUERY,
                map("name", JDBC_NAME, "map", getVirtualizeJDBCParameterMap(mysql, JDBC_SELECT_QUERY)));

        testCallEmpty(db, "CALL apoc.dv.catalog.remove($name)", map("name", JDBC_NAME));
    }

    @Test
    public void testNameAsKey() {
        Map<String, Object> params = map(
                NAME_KEY, JDBC_NAME, "map", getVirtualizeJDBCParameterMap(mysql, JDBC_SELECT_QUERY)
        );

        db.executeTransactionally(APOC_DV_ADD_QUERY, params);
        db.executeTransactionally(APOC_DV_ADD_QUERY, params);
        testResult(db, "CALL apoc.dv.catalog.list()",
                map(),
                (result) -> assertEquals(1, result.stream().count()));
    }

    @Test
    public void testJDBCQueryWithMixedParamsTypes() {
        try {
            db.executeTransactionally(APOC_DV_ADD_QUERY,
                    map("name", JDBC_NAME, "map", getVirtualizeJDBCParameterMap(mysql, JDBC_SELECT_QUERY_WITH_PARAM)));
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
                map("name", JDBC_NAME, "map", getVirtualizeJDBCParameterMap(mysql, VIRTUALIZE_JDBC_WITH_PARAMS_QUERY)),
                (row) -> assertDvCatalogAddOrInstall(row, url));

        String country = "Netherlands";
        String code2 = "NL";
        String headOfState = "Beatrix";
        Map<String, Object> queryParams = map("foo", country, "bar", code2, "baz", headOfState);

        try {
            db.executeTransactionally(APOC_DV_QUERY,
                    map(NAME_KEY, JDBC_NAME, APOC_DV_QUERY_PARAMS_KEY, queryParams,
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
