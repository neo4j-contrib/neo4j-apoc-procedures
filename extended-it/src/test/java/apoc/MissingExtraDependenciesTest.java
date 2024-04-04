package apoc;

import apoc.util.MissingDependencyException;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Session;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import static apoc.couchbase.Couchbase.COUCHBASE_MISSING_DEPS_ERROR;
import static apoc.data.email.ExtractEmail.EMAIL_MISSING_DEPS_ERROR;
import static apoc.export.parquet.ParquetConfig.PARQUET_MISSING_DEPS_ERROR;
import static apoc.export.xls.ExportXlsHandler.XLS_MISSING_DEPS_ERROR;
import static apoc.load.LoadHtml.SELENIUM_MISSING_DEPS_ERROR;
import static apoc.mongodb.MongoDBUtils.MONGO_MISSING_DEPS_ERROR;
import static apoc.redis.RedisConfig.REDIS_MISSING_DEPS_ERROR;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This test verifies that, if the `extra-dependencies` jars are not present, 
 * the procedures that require them fail with {@link apoc.util.MissingDependencyException}
 */
public class MissingExtraDependenciesTest {
    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void setUp() throws Exception {
        neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.EXTENDED), true);
        neo4jContainer.start();

        session = neo4jContainer.getSession();
    }

    @AfterClass
    public static void tearDown() {
        session.close();
        neo4jContainer.close();
    }

    @Test
    public void testParquet() {
        // export file
        assertParquetFails("CALL apoc.export.parquet.all('test.parquet')");
        assertParquetFails("CALL apoc.export.parquet.data([], [], 'test.parquet')");
        assertParquetFails("CALL apoc.export.parquet.graph({nodes: [], relationships: []}, 'test.parquet')");
        assertParquetFails("CALL apoc.export.parquet.query('MATCH (n:ParquetNode) RETURN n', 'test.parquet')");

        // export stream
        assertParquetFails("CALL apoc.export.parquet.all.stream()");
        assertParquetFails("CALL apoc.export.parquet.data.stream([], [])");
        assertParquetFails("CALL apoc.export.parquet.graph.stream({nodes: [], relationships: []})");
        assertParquetFails("CALL apoc.export.parquet.query.stream('MATCH (n:ParquetNode) RETURN n')");

        // import and load
        assertParquetFails("CALL apoc.import.parquet('test.parquet')");
        assertParquetFails("CALL apoc.load.parquet('test.parquet')");
    }

    private static void assertParquetFails(String query) {
        assertFails(query, PARQUET_MISSING_DEPS_ERROR);
    }

    @Test
    public void testCouchbase() {
        assertCouchbaseFails("CALL apoc.couchbase.get('host', 'bucket', 'documentId')");
        assertCouchbaseFails("CALL apoc.couchbase.exists('host', 'bucket', 'documentId')");
        assertCouchbaseFails("CALL apoc.couchbase.remove('host', 'bucket', 'documentId')");
        assertCouchbaseFails("CALL apoc.couchbase.replace('host', 'bucket', 'documentId', '{}')");
        assertCouchbaseFails("CALL apoc.couchbase.insert('host', 'bucket', 'documentId', '{}')");
        assertCouchbaseFails("CALL apoc.couchbase.upsert('host', 'bucket', 'documentId', '{}')");
        assertCouchbaseFails("CALL apoc.couchbase.query('host', 'bucket', 'documentId')");
        assertCouchbaseFails("CALL apoc.couchbase.posParamsQuery('host', 'bucket', 'documentId', [''])");
        assertCouchbaseFails("CALL apoc.couchbase.namedParamsQuery('host', 'bucket', 'documentId', [''], [''])");
        
        Map<String, Object> params = Map.of("content", "zzz".getBytes());
        assertFails("CALL apoc.couchbase.append('host', 'bucket', 'documentId', $content)", params, COUCHBASE_MISSING_DEPS_ERROR);
        assertFails("CALL apoc.couchbase.prepend('host', 'bucket', 'documentId', $content)", params, COUCHBASE_MISSING_DEPS_ERROR);
    }

    private static void assertCouchbaseFails(String query) {
        assertFails(query, COUCHBASE_MISSING_DEPS_ERROR);
    }

    @Test
    public void testRedis() {
        assertRedisFails("CALL apoc.redis.getSet('host', 'key', 'value')");
        assertRedisFails("CALL apoc.redis.get('host', 'key')");
        assertRedisFails("CALL apoc.redis.append('host', 'key', 1)");
        assertRedisFails("CALL apoc.redis.incrby('host', 'key', 1)");
        assertRedisFails("CALL apoc.redis.hdel('host', 'key', ['1'])");
        assertRedisFails("CALL apoc.redis.hexists('host', 'key', 'field')");
        assertRedisFails("CALL apoc.redis.hget('host', 'key', 1)");
        assertRedisFails("CALL apoc.redis.hincrby('host', 'key', '1', 1)");
        assertRedisFails("CALL apoc.redis.hgetall('host', 'key')");
        assertRedisFails("CALL apoc.redis.hset('host', 'key', '1', '1')");
        assertRedisFails("CALL apoc.redis.push('host', 'key', ['1'])");
        assertRedisFails("CALL apoc.redis.pop('host', 'key')");
        assertRedisFails("CALL apoc.redis.lrange('host', 'key', 1, 2)");
        assertRedisFails("CALL apoc.redis.sadd('host', 'key', [1, 2])");
        assertRedisFails("CALL apoc.redis.scard('host', 'key')");
        assertRedisFails("CALL apoc.redis.spop('host', 'key')");
        assertRedisFails("CALL apoc.redis.smembers('host', 'key')");
        assertRedisFails("CALL apoc.redis.sunion('host', ['key'])");
        assertRedisFails("CALL apoc.redis.zadd('host', 'key', ['key'])");
        assertRedisFails("CALL apoc.redis.zcard('host', 'key')");
        assertRedisFails("CALL apoc.redis.zrangebyscore('host', 'key', 1, 2)");
        assertRedisFails("CALL apoc.redis.zrem('host', 'key', [1, 2])");
        assertRedisFails("CALL apoc.redis.eval('host', 'key', 'type', ['keys'], ['vals'])");
        assertRedisFails("CALL apoc.redis.copy('host', 'src', 'dest')");
        assertRedisFails("CALL apoc.redis.exists('host', ['src'])");
        assertRedisFails("CALL apoc.redis.exists('host', ['key'])");
        assertRedisFails("CALL apoc.redis.persist('host', 'key')");
        assertRedisFails("CALL apoc.redis.pttl('host', 'key')");
        assertRedisFails("CALL apoc.redis.info('host')");
        assertRedisFails("CALL apoc.redis.configGet('host', 'par')");
        assertRedisFails("CALL apoc.redis.configSet('host', 'par', 'val')");
    }

    private static void assertRedisFails(String query) {
        assertFails(query, REDIS_MISSING_DEPS_ERROR);
    }

    @Test
    public void testMongoDb() {
        assertMongoFails("CALL apoc.mongo.aggregate('uri', [{a: '1'}])");
        assertMongoFails("CALL apoc.mongo.count('uri', 'query')");
        assertMongoFails("CALL apoc.mongo.find('uri', 'query')");
        assertMongoFails("CALL apoc.mongo.insert('uri', ['docs'])");
        assertMongoFails("CALL apoc.mongo.update('uri', 'query', 'update')");
        assertMongoFails("CALL apoc.mongo.delete('uri', 'query')");
        assertMongoFails("CALL apoc.mongodb.get.byObjectId('uri', 'db', 'coll', 'objIdVal')");
    }

    private static void assertMongoFails(String query) {
        assertFails(query, MONGO_MISSING_DEPS_ERROR);
    }

    @Test
    public void testEmail() {
        assertFails("RETURN apoc.data.email('email@gmail.com')", EMAIL_MISSING_DEPS_ERROR);
    }

    @Test
    public void testSelenium() {
        assertFails("CALL apoc.load.html('https://www.google.com/', {a: 'a'}, {browser: 'CHROME'})", SELENIUM_MISSING_DEPS_ERROR);
        assertFails("CALL apoc.load.html('https://www.google.com/', {a: 'a'}, {browser: 'FIREFOX'})", SELENIUM_MISSING_DEPS_ERROR);
    }

    @Test
    public void testXls() throws MalformedURLException {
        String url = new URL("https://github.com/neo4j-contrib/neo4j-apoc-procedures/raw/5.13/extended/src/test/resources/load_test.xlsx")
                .toString();
        assertFails("CALL apoc.load.xls($url,'sheet')", Map.of("url", url), XLS_MISSING_DEPS_ERROR);
        assertFails("CALL apoc.export.xls.all('file.xls', {})", XLS_MISSING_DEPS_ERROR);
    }

    private static void assertFails(String query, String errorMessage) {
        assertFails(query, Map.of(), errorMessage, session);
    }

    private static void assertFails(String query, Map<String, Object> params, String errorMessage) {
        assertFails(query, params, errorMessage, session);
    }

    public static void assertFails(String query, Map<String, Object> params, String errorMessage, Session session) {
        try {
            testCall(session, query, params, (row) ->fail("Should fail due to `MissingDependencyException`"));
        } catch (RuntimeException e) {
            String message = e.getMessage();
            // String of type `apoc.util.MissingDependencyException: <errorMessage>`
            String expected = "%s: %s".formatted(MissingDependencyException.class.getName(), errorMessage);
            assertTrue("Actual error message is: " + message, message.contains(expected));
        }
    }

}
