package apoc.couchbase;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.TestGraphDatabaseFactory;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

import apoc.util.TestUtil;

public class CouchbaseIT extends CouchbaseAbstractTest {

  protected static GraphDatabaseService graphDB;

  @BeforeClass
  public static void setUp() throws Exception {
    CouchbaseAbstractTest.setUp();
    assumeTrue(couchbaseRunning);
    if (couchbaseRunning) {
      graphDB = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase();
      TestUtil.registerProcedure(graphDB, Couchbase.class);
    }
  }

  @AfterClass
  public static void tearDown() {
    CouchbaseAbstractTest.tearDown();
    if (graphDB != null) {
      graphDB.shutdown();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetViaCall() throws Exception {
    try {
      //@eclipse-formatter:off
      TestUtil.testCall(graphDB, "CALL apoc.couchbase.get(['localhost'], 'default', 'artist:vincent_van_gogh')", r -> {
        assertTrue(r.get("content") instanceof Map);
        Map<String, Object> content = (Map<String, Object>) r.get("content");
        assertTrue(content.get("notableWorks") instanceof List);
        List<String> notableWorks = (List<String>) content.get("notableWorks");
				checkDocumentMetadata(
						(String) r.get("id"),
						(long) r.get("expiry"),
						(long) r.get("cas"),
						(Map<String, Object>) r.get("mutationToken"));
				checkDocumentContent(
						(String) content.get("firstName"),
						(String) content.get("secondName"),
						(String) content.get("lastName"),
						notableWorks);
      });
      //@eclipse-formatter:on
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testExistsViaCall() throws Exception {
    try {
      //@eclipse-formatter:off
      TestUtil.testCall(graphDB, "CALL apoc.couchbase.exists(['localhost'], 'default', 'artist:vincent_van_gogh')", r -> {
        assertTrue((boolean) r.get("value"));
      });
      //@eclipse-formatter:on
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testInsertViaCall() {
    try {
      //@eclipse-formatter:off
      TestUtil.testCall(graphDB, "CALL apoc.couchbase.insert(['localhost'], 'default', 'testInsertViaCall', '" + jsonDocumentCreatedForThisTest.content().toString() + "')", r -> {
        assertTrue(r.get("content") instanceof Map);
        Map<String, Object> content = (Map<String, Object>) r.get("content");
        assertTrue(content.get("notableWorks") instanceof List);
        List<String> notableWorks = (List<String>) content.get("notableWorks");
        checkDocumentContent(
            (String) content.get("firstName"),
            (String) content.get("secondName"),
            (String) content.get("lastName"),
            notableWorks);
      //@eclipse-formatter:on
      });
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    } finally {
      couchbaseBucket.remove("testInsertViaCall");
      assertFalse(couchbaseBucket.exists("testInsertViaCall"));
    }
  }
  
  @Test
  public void testInsertWithAlreadyExistingIDViaCall() {
    expectedEx.expect(QueryExecutionException.class);
    //@eclipse-formatter:off
    TestUtil.testCall(graphDB, "CALL apoc.couchbase.insert(['localhost'], 'default', 'artist:vincent_van_gogh', '" + jsonDocumentCreatedForThisTest.content().toString() + "')", r -> {});
    //@eclipse-formatter:on
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void testUpsertViaCall() {
    try {
      //@eclipse-formatter:off
      TestUtil.testCall(graphDB, "CALL apoc.couchbase.upsert(['localhost'], 'default', 'testUpsertViaCall', '" + jsonDocumentCreatedForThisTest.content().toString() + "')", r -> {
        assertTrue(r.get("content") instanceof Map);
        Map<String, Object> content = (Map<String, Object>) r.get("content");
        assertTrue(content.get("notableWorks") instanceof List);
        List<String> notableWorks = (List<String>) content.get("notableWorks");
        checkDocumentContent(
            (String) content.get("firstName"),
            (String) content.get("secondName"),
            (String) content.get("lastName"),
            notableWorks);
      //@eclipse-formatter:on
      });
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    } finally {
      couchbaseBucket.remove("testUpsertViaCall");
      assertFalse(couchbaseBucket.exists("testUpsertViaCall"));
    }
  }

  @Test
  public void testRemoveViaCall() {
    try {
      //@eclipse-formatter:off
      couchbaseBucket.insert(JsonDocument.create("testRemove", JsonObject.empty()));
      TestUtil.testCall(graphDB, "CALL apoc.couchbase.remove(['localhost'], 'default', 'testRemove')", r -> {});
      assertFalse(couchbaseBucket.exists("testRemove"));
      //@eclipse-formatter:on
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
  
  @Test
  public void testQueryViaCall() {
    try {
      //@eclipse-formatter:off
      TestUtil.testCall(graphDB, "CALL apoc.couchbase.query(['localhost'], 'default', 'select * from default where lastName = \"Van Gogh\"')", r -> {
        checkListResult(r);
      });
      //@eclipse-formatter:on
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testQueryWithPositionalParamsViaCall() {
    try {
      //@eclipse-formatter:off
      TestUtil.testCall(graphDB, "CALL apoc.couchbase.posParamsQuery(['localhost'], 'default', 'select * from default where lastName = $1', ['Van Gogh'])", r -> {
        checkListResult(r);
      });
      //@eclipse-formatter:on
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testQueryWithNamedParamsViaCall() {
    try {
      //@eclipse-formatter:off
      TestUtil.testCall(graphDB, "CALL apoc.couchbase.namedParamsQuery(['localhost'], 'default', 'select * from default where lastName = $lastName', ['lastName'], ['Van Gogh'])", r -> {
        checkListResult(r);
      });
      //@eclipse-formatter:on
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}