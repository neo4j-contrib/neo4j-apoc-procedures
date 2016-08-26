package apoc.couchbase;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.junit.BeforeClass;
import org.junit.Test;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;

import apoc.couchbase.document.CouchbaseJsonDocument;
import apoc.couchbase.document.CouchbaseQueryResult;
import apoc.result.BooleanResult;

public class CouchbaseTest extends CouchbaseAbstractTest {

  @BeforeClass
  public static void setUp() throws Exception {
    CouchbaseAbstractTest.setUp();
    assumeTrue(couchbaseRunning);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGet() {
    try {
      //@eclipse-formatter:off
      Stream<CouchbaseJsonDocument> stream = new Couchbase().get(Arrays.asList("localhost"), "default", "artist:vincent_van_gogh");
      Iterator<CouchbaseJsonDocument> iterator = stream.iterator();
      assertTrue(iterator.hasNext());
      CouchbaseJsonDocument couchbaseJsonDocument = iterator.next();
			checkDocumentMetadata(
					couchbaseJsonDocument.getId(),
					couchbaseJsonDocument.getExpiry(),
					couchbaseJsonDocument.getCas(),
					couchbaseJsonDocument.getMutationToken());
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
      Stream<BooleanResult> stream = new Couchbase().exists(Arrays.asList("localhost"), "default", "artist:vincent_van_gogh");
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
      Stream<CouchbaseJsonDocument> stream = new Couchbase().insert(Arrays.asList("localhost"), "default", "testInsert", jsonDocumentCreatedForThisTest.content().toString());
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

  @Test
  public void testInsertWithAlreadyExistingID() {
    expectedEx.expect(DocumentAlreadyExistsException.class);
    //@eclipse-formatter:off
    new Couchbase().insert(Arrays.asList("localhost"), "default", "artist:vincent_van_gogh", JsonObject.empty().toString());
    //@eclipse-formatter:on
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testUpsert() {
    try {
      //@eclipse-formatter:off
      Stream<CouchbaseJsonDocument> stream = new Couchbase().upsert(Arrays.asList("localhost"), "default", "testUpsert", jsonDocumentCreatedForThisTest.content().toString());
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
      new Couchbase().remove(Arrays.asList("localhost"), "default", "testRemove");
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
      Stream<CouchbaseQueryResult> stream = new Couchbase().query(Arrays.asList("localhost"), "default", "select * from default where lastName = 'Van Gogh'");
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
      Stream<CouchbaseQueryResult> stream = new Couchbase().posParamsQuery(Arrays.asList("localhost"), "default", "select * from default where lastName = $1", Arrays.asList("Van Gogh"));
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
      Stream<CouchbaseQueryResult> stream = new Couchbase().namedParamsQuery(Arrays.asList("localhost"), "default", "select * from default where lastName = $lastName", Arrays.asList("lastName"), Arrays.asList("Van Gogh"));
      //@eclipse-formatter:on
      checkQueryResult(stream);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}