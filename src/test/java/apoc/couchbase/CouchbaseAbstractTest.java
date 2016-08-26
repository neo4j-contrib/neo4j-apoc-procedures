package apoc.couchbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.consistency.ScanConsistency;

import apoc.couchbase.document.CouchbaseQueryResult;
import apoc.util.TestUtil;

public class CouchbaseAbstractTest {

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  protected static Cluster couchbaseCluster;
  protected static Bucket couchbaseBucket;
  protected static boolean couchbaseRunning = false;
  protected static JsonDocument jsonDocumentCreatedForThisTest;

  protected CouchbaseAbstractTest() {
  }

  //@eclipse-formatter:off
  
  @BeforeClass
  @SuppressWarnings("unchecked")
  public static void setUp() throws Exception {
  	JsonObject vincentVanGogh =
  			JsonObject.create()
  			          .put("firstName", "Vincent")
  			          .put("secondName", "Willem")
  			          .put("lastName", "Van Gogh")
  			          .put("notableWorks", JsonArray.from("Starry Night", "Sunflowers", "Bedroom in Arles", "Portrait of Dr Gachet", "Sorrow"));
    
    TestUtil.ignoreException(() -> {
      couchbaseCluster = CouchbaseCluster.create(CouchbaseManager.DEFAULT_COUCHBASE_ENVIRONMENT, "localhost");
      couchbaseBucket = couchbaseCluster.openBucket("default");
      jsonDocumentCreatedForThisTest = couchbaseBucket.upsert(JsonDocument.create("artist:vincent_van_gogh", vincentVanGogh));
      couchbaseBucket.bucketManager().createN1qlPrimaryIndex(true, false);
      N1qlQueryResult queryResult = couchbaseBucket.query(N1qlQuery.simple("select * from default where lastName = 'Van Gogh'", N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS)));
      couchbaseRunning = queryResult.info().resultCount() == 1;
    }, TimeoutException.class, CouchbaseException.class);
  }

  @AfterClass
  public static void tearDown() {
    if (couchbaseBucket != null) {
      couchbaseBucket.remove(jsonDocumentCreatedForThisTest);
    }
    if (couchbaseCluster != null) {
      couchbaseCluster.disconnect();
    }
  }
  
  //@eclipse-formatter:on

  //
  // Common Test Assertions
  //

  @SuppressWarnings("unchecked")
  protected void checkListResult(Map<String, Object> r) {
    assertTrue(r.get("queryResult") instanceof List);
    List<Map<String, Object>> listResult = (List<Map<String, Object>>) r.get("queryResult");
    assertNotNull(listResult);
    assertEquals(1, listResult.size());
    assertTrue(listResult.get(0) instanceof Map);
    Map<String, Object> result = (Map<String, Object>) listResult.get(0);
    checkResult(result);
  }

  protected void checkQueryResult(Stream<CouchbaseQueryResult> stream) {
    Iterator<CouchbaseQueryResult> iterator = stream.iterator();
    assertTrue(iterator.hasNext());
    CouchbaseQueryResult queryResult = iterator.next();
    assertNotNull(queryResult);
    assertEquals(1, queryResult.queryResult.size());
    assertTrue(queryResult.queryResult.get(0) instanceof Map);
    Map<String, Object> result = (Map<String, Object>) queryResult.queryResult.get(0);
    checkResult(result);
  }

  @SuppressWarnings("unchecked")
  protected void checkResult(Map<String, Object> result) {
    assertTrue(result.get("default") instanceof Map);
    Map<String, Object> content = (Map<String, Object>) result.get("default");
    assertTrue(content.get("notableWorks") instanceof List);
    List<String> notableWorks = (List<String>) content.get("notableWorks");
    //@eclipse-formatter:off
    checkDocumentContent(
      (String) content.get("firstName"),
      (String) content.get("secondName"),
      (String) content.get("lastName"),
      notableWorks);
    //@eclipse-formatter:on
  }

  protected void checkDocumentMetadata(String id, long expiry, long cas, Map<String, Object> mutationToken) {
    assertEquals(jsonDocumentCreatedForThisTest.id(), id);
    assertEquals(jsonDocumentCreatedForThisTest.expiry(), expiry);
    assertEquals(jsonDocumentCreatedForThisTest.cas(), cas);
    assertEquals(jsonDocumentCreatedForThisTest.mutationToken(), mutationToken);
  }

  protected void checkDocumentContent(String firstName, String secondName, String lastName, List<String> notableWorks) {
    assertEquals("Vincent", firstName);
    assertEquals("Willem", secondName);
    assertEquals("Van Gogh", lastName);
    assertEquals(5, notableWorks.size());
    assertTrue(notableWorks.contains("Starry Night"));
    assertTrue(notableWorks.contains("Sunflowers"));
    assertTrue(notableWorks.contains("Bedroom in Arles"));
    assertTrue(notableWorks.contains("Portrait of Dr Gachet"));
    assertTrue(notableWorks.contains("Sorrow"));
  }
}