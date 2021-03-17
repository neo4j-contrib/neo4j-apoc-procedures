package apoc.couchbase;

import apoc.couchbase.document.CouchbaseJsonDocument;
import apoc.couchbase.document.CouchbaseQueryResult;
import apoc.result.BooleanResult;
import com.couchbase.client.core.error.DocumentExistsException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ApocConfig.apocConfig;
import static apoc.couchbase.CouchbaseTestUtils.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Ignore // The same tests are covered from CouchbaseIT, for now we disable this in order to reduce the build time
public class CouchbaseTest {

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() {
        createCouchbaseContainer();

        // if we have several hosts comma separated, we can connect if at least one of them is valid
        Map<String, Object> properties = Map.of(
                BASE_CONFIG_KEY + CouchbaseManager.URI_CONFIG_KEY, "invalidOne,localhost,invalidTwo",
                BASE_CONFIG_KEY + CouchbaseManager.USERNAME_CONFIG_KEY, USERNAME,
                BASE_CONFIG_KEY + CouchbaseManager.PASSWORD_CONFIG_KEY, PASSWORD,
                BASE_CONFIG_KEY + CouchbaseManager.PORT_CONFIG_KEY, couchbase.getMappedPort(8091)
        );
        properties.forEach((key, value) -> apocConfig().setProperty(key, value));
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
            collection.remove("testInsert");
            assertFalse(collection.exists("testInsert").exists());
        }
    }

    @Test(expected = DocumentExistsException.class)
    public void testInsertWithAlreadyExistingID() {
        //@eclipse-formatter:off
        new Couchbase().insert(HOST, BUCKET_NAME, "artist:vincent_van_gogh", "{}");
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
            collection.remove("testUpsert");
            assertFalse(collection.exists("testUpsert").exists());
        }
    }

    @Test
    public void testRemove() {
        try {
            //@eclipse-formatter:off
            collection.insert("testRemove", "");
            new Couchbase().remove(HOST, BUCKET_NAME, "testRemove");
            assertFalse(collection.exists("testRemove").exists());
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