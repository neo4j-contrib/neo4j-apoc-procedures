package apoc.data;

import apoc.util.TestUtil;
import com.sun.tools.javac.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import static apoc.util.TestUtil.testCall;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;

public class AggregateTest {

    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Aggregate.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testFirst() {
        testCall(db, "RETURN apoc.data.first(['1', '2', '3', '4', '5', '6']) AS value", row -> Assert.assertThat(row.get("value"), equalTo("1")));
    }

    @Test
    public void testLast() {
        testCall(db, "RETURN apoc.data.last(['1', '2', '3', '4', '5', '6']) AS value", row -> Assert.assertThat(row.get("value"), equalTo("6")));
    }

    @Test
    public void testNth() {
        testCall(db, "RETURN apoc.data.nth(['1', '2', '3', '4', '5', '6'], -1) AS value", row -> assertEquals(null, row.get("value")));
        testCall(db, "RETURN apoc.data.nth(['1', '2', '3', '4', '5', '6'], 1) AS value", row -> assertEquals("2", row.get("value")));
        testCall(db, "RETURN apoc.data.nth(['1', '2', '3', '4', '5', '6'], 10) AS value", row -> assertEquals(null, row.get("value")));
    }

    @Test
    public void testSubset() {
        testCall(db, "RETURN apoc.data.subset(['1', '2', '3', '4', '5', '6'], 1, 2) AS value", row -> assertEquals(List.of("2", "3"), row.get("value")));
        testCall(db, "RETURN apoc.data.subset(['1', '2', '3', '4', '5', '6'], 5, 5) AS value", row -> assertEquals(null, row.get("value")));
        testCall(db, "RETURN apoc.data.subset(['1', '2', '3', '4', '5', '6'], 1, -1) AS value", row -> assertEquals(null, row.get("value")));
        testCall(db, "RETURN apoc.data.subset(['1', '2', '3', '4', '5', '6'], -1, 2) AS value", row -> assertEquals(null, row.get("value")));
    }
}
