package apoc.es;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.net.ConnectException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

/**
 * @author mh
 * @since 21.05.16
 */
public class ElasticSearchTest {

    private final static String HOST = "localhost";
    private final static String ES_INDEX = "test-index";
    private final static String ES_TYPE = "test-type";
    private final static String ES_ID = "1";

    private static GraphDatabaseService db;

    // We need a reference to the class implementing the procedures
    private final ElasticSearch es = new ElasticSearch();

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .newGraphDatabase();
        TestUtil.registerProcedure(db, ElasticSearch.class);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testStats() throws Exception {
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.es.stats(null)", r -> assertNotNull(r.get("value")));
        }, ConnectException.class);
    }

    @Test
    public void testGetQueryUrlShouldBeTheSameAsOldFormatting() {
        String index = ES_INDEX;
        String type = ES_TYPE;
        String id = ES_ID;
        Map<String, String> query = new HashMap<>();
        query.put("name", "get");

        String host = HOST;
        String hostUrl = es.getElasticSearchUrl(host);

        String queryUrl = hostUrl + String.format("/%s/%s/%s?%s", index == null ? "_all" : index,
                type == null ? "_all" : type,
                id == null ? "" : id,
                es.toQueryParams(query));

        Assert.assertEquals(queryUrl, es.getQueryUrl(host, index, type, id, query));
    }

    @Test
    public void testGetQueryUrlShouldNotHaveTrailingQuestionMarkIfQueryIsNull() {
        String index = ES_INDEX;
        String type = ES_TYPE;
        String id = ES_TYPE;

        String host = HOST;
        String hostUrl = es.getElasticSearchUrl(host);
        String queryUrl = hostUrl + String.format("/%s/%s/%s?%s", index == null ? "_all" : index,
                type == null ? "_all" : type,
                id == null ? "" : id,
                es.toQueryParams(null));

        // First we test the older version against the newest one
        Assert.assertNotEquals(queryUrl, es.getQueryUrl(host, index, type, id, null));
        Assert.assertTrue(!es.getQueryUrl(host, index, type, id, null).endsWith("?"));
    }

    @Test
    public void testGetQueryUrlShouldNotHaveTrailingQuestionMarkIfQueryIsEmpty() {
        String index = ES_INDEX;
        String type = ES_TYPE;
        String id = ES_ID;

        String host = HOST;
        String hostUrl = es.getElasticSearchUrl(host);
        String queryUrl = hostUrl + String.format("/%s/%s/%s?%s", index == null ? "_all" : index,
                type == null ? "_all" : type,
                id == null ? "" : id,
                es.toQueryParams(new HashMap<String, String>()));

        // First we test the older version against the newest one
        Assert.assertNotEquals(queryUrl, es.getQueryUrl(host, index, type, id, new HashMap<String, String>()));
        Assert.assertTrue(!es.getQueryUrl(host, index, type, id, new HashMap<String, String>()).endsWith("?"));
    }
}
