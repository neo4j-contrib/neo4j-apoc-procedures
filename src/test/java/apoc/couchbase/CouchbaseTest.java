package apoc.couchbase;

import apoc.couchbase.document.CouchbaseJsonDocument;
import apoc.couchbase.document.CouchbaseQueryResult;
import apoc.result.BooleanResult;
import apoc.util.TestUtil;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import org.junit.*;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.couchbase.CouchbaseContainer;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static apoc.ApocSettings.dynamic;
import static apoc.couchbase.CouchbaseTestUtils.*;
import static apoc.util.TestUtil.isTravis;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.neo4j.configuration.SettingValueParsers.STRING;

@Ignore // The same tests are covered from CouchbaseIT, for now we disable this in order to reduce the build time
public class CouchbaseTest {

    private static String HOST = null;

    private static Bucket couchbaseBucket;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(dynamic("apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + CONNECTION_TIMEOUT_CONFIG_KEY, STRING), CONNECTION_TIMEOUT_CONFIG_VALUE)
            .withSetting(dynamic("apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + SOCKET_CONNECT_TIMEOUT_CONFIG_KEY,STRING), SOCKET_CONNECT_TIMEOUT_CONFIG_VALUE)
            .withSetting(dynamic("apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + KV_TIMEOUT_CONFIG_KEY, STRING), KV_TIMEOUT_CONFIG_VALUE);

    @ClassRule
    public static CouchbaseContainer couchbase;

    @BeforeClass
    public static void setUp() {
        assumeFalse(isTravis());
        TestUtil.ignoreException(() -> {
            couchbase = new CouchbaseContainer()
                    .withClusterAdmin(USERNAME, PASSWORD)
                    .withNewBucket(DefaultBucketSettings.builder()
                            .password(PASSWORD)
                            .name(BUCKET_NAME)
                            .quota(100)
                            .type(BucketType.COUCHBASE)
                            .build());
            couchbase.start();
        }, Exception.class);
        assumeNotNull(couchbase);
        assumeTrue("couchbase must be running", couchbase.isRunning());
        boolean isFilled = fillDB(couchbase.getCouchbaseCluster());
        assumeTrue("should fill Couchbase with data", isFilled);
        HOST = getUrl(couchbase);
        couchbaseBucket = getCouchbaseBucket(couchbase);
    }

    @AfterClass
    public static void tearDown() {
        if (couchbase != null) {
            couchbase.stop();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGet() {
        try {
            //@eclipse-formatter:off
            Stream<CouchbaseJsonDocument> stream = new Couchbase().get(HOST, BUCKET_NAME, "artist:vincent_van_gogh");
            Iterator<CouchbaseJsonDocument> iterator = stream.iterator();
            assertTrue(iterator.hasNext());
            CouchbaseJsonDocument couchbaseJsonDocument = iterator.next();
            checkDocumentContent(
                    (String) couchbaseJsonDocument.getContent().get("firstName"),
                    (String) couchbaseJsonDocument.getContent().get("secondName"),
                    (String) couchbaseJsonDocument.getContent().get("lastName"),
                    (List<String>) couchbaseJsonDocument.getContent().get("notableWorks"));
            assertFalse(iterator.hasNext());
            //@eclipse-formatter:on
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testExists() {
        try {
            //@eclipse-formatter:off
            Stream<BooleanResult> stream = new Couchbase().exists(HOST, BUCKET_NAME, "artist:vincent_van_gogh");
            //@eclipse-formatter:on
            Iterator<BooleanResult> iterator = stream.iterator();
            assertTrue(iterator.hasNext());
            BooleanResult booleanResult = iterator.next();
            assertTrue(booleanResult.value);
            assertFalse(iterator.hasNext());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testInsert() {
        try {
            //@eclipse-formatter:off
            Stream<CouchbaseJsonDocument> stream = new Couchbase().insert(HOST, BUCKET_NAME, "testInsert", VINCENT_VAN_GOGH.toString());
            Iterator<CouchbaseJsonDocument> iterator = stream.iterator();
            assertTrue(iterator.hasNext());
            CouchbaseJsonDocument couchbaseJsonDocument = iterator.next();
            checkDocumentContent(
                    (String) couchbaseJsonDocument.getContent().get("firstName"),
                    (String) couchbaseJsonDocument.getContent().get("secondName"),
                    (String) couchbaseJsonDocument.getContent().get("lastName"),
                    (List<String>) couchbaseJsonDocument.getContent().get("notableWorks"));
            assertFalse(iterator.hasNext());
            //@eclipse-formatter:on
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            couchbaseBucket.remove("testInsert");
            assertFalse(couchbaseBucket.exists("testInsert"));
        }
    }

    @Test(expected = DocumentAlreadyExistsException.class)
    public void testInsertWithAlreadyExistingID() {
        //@eclipse-formatter:off
        new Couchbase().insert(HOST, BUCKET_NAME, "artist:vincent_van_gogh", JsonObject.empty().toString());
        //@eclipse-formatter:on
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testUpsert() {
        try {
            //@eclipse-formatter:off
            Stream<CouchbaseJsonDocument> stream = new Couchbase().upsert(HOST, BUCKET_NAME, "testUpsert", VINCENT_VAN_GOGH.toString());
            Iterator<CouchbaseJsonDocument> iterator = stream.iterator();
            assertTrue(iterator.hasNext());
            CouchbaseJsonDocument couchbaseJsonDocument = iterator.next();
            checkDocumentContent(
                    (String) couchbaseJsonDocument.getContent().get("firstName"),
                    (String) couchbaseJsonDocument.getContent().get("secondName"),
                    (String) couchbaseJsonDocument.getContent().get("lastName"),
                    (List<String>) couchbaseJsonDocument.getContent().get("notableWorks"));
            assertFalse(iterator.hasNext());
            //@eclipse-formatter:on
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            couchbaseBucket.remove("testUpsert");
            assertFalse(couchbaseBucket.exists("testUpsert"));
        }
    }

    @Test
    public void testRemove() {
        try {
            //@eclipse-formatter:off
            couchbaseBucket.insert(JsonDocument.create("testRemove", JsonObject.empty()));
            new Couchbase().remove(HOST, BUCKET_NAME, "testRemove");
            assertFalse(couchbaseBucket.exists("testRemove"));
            //@eclipse-formatter:on
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testQuery() {
        try {
            //@eclipse-formatter:off
            Stream<CouchbaseQueryResult> stream = new Couchbase().query(HOST, BUCKET_NAME, "select * from " + BUCKET_NAME + " where lastName = 'Van Gogh'");
            //@eclipse-formatter:on
            checkQueryResult(stream);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testQueryWithPositionalParams() {
        try {
            //@eclipse-formatter:off
            Stream<CouchbaseQueryResult> stream = new Couchbase().posParamsQuery(HOST, BUCKET_NAME, "select * from " + BUCKET_NAME + " where lastName = $1", Arrays.asList("Van Gogh"));
            //@eclipse-formatter:on
            checkQueryResult(stream);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testQueryWithNamedParams() {
        try {
            //@eclipse-formatter:off
            Stream<CouchbaseQueryResult> stream = new Couchbase().namedParamsQuery(HOST, BUCKET_NAME, "select * from " + BUCKET_NAME + " where lastName = $lastName", Arrays.asList("lastName"), Arrays.asList("Van Gogh"));
            //@eclipse-formatter:on
            checkQueryResult(stream);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }
}
