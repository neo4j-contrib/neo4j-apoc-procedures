package apoc.data.email;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import static apoc.util.TestUtil.testCall;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;

public class ExtractEmailTest {
    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, ExtractEmail.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testPersonalPresent() {
        testCall(db, "RETURN apoc.data.email.personal('David <david.allen@neo4j.com>') AS value",
            row -> assertEquals("David", row.get("value")));
    }

    @Test
    public void testPersonalMissing() {
        testCall(db, "RETURN apoc.data.email.personal('<david.allen@neo4j.com>') AS value",
            row -> assertEquals(null, row.get("value")));
    }

    @Test
    public void testUser() {
        testCall(db, "RETURN apoc.data.email.user('Unusual <example-throwaway@gmail.com>') AS value",
        row -> assertEquals("example-throwaway", row.get("value")));    
    }

    @Test
    public void testMissingUser() {
        testCall(db, "RETURN apoc.data.email.user('mail.com') AS value",
        row -> assertEquals(null, row.get("value")));    
    }

    @Test
    public void testQuotedEmail() {
        testCall(db, "RETURN apoc.data.email.domain('<foo@bar.baz>') AS value",
                row -> Assert.assertThat(row.get("value"), equalTo("bar.baz")));
    }

    @Test
    public void testEmail() {
        testCall(db, "RETURN apoc.data.email.domain('foo@bar.baz') AS value",
                row -> Assert.assertThat(row.get("value"), equalTo("bar.baz")));
    }

    @Test
    public void testLocalEmail() {
        // Internet standards strongly discourage this possibility, but it's out there.
        testCall(db, "RETURN apoc.data.email.domain('root@localhost') AS value",
        row -> Assert.assertThat(row.get("value"), equalTo("localhost")));
    }

    @Test
    public void testNull() {
        testCall(db, "RETURN apoc.data.email.domain(null) AS value",
                row -> assertEquals(null, row.get("value")));
    }

    @Test
    public void testBadString() {
        testCall(db, "RETURN apoc.data.email.domain('asdsgawe4ge') AS value",
                row -> assertEquals(null, row.get("value")));
    }

    @Test
    public void testEmailWithDotsBeforeAt() {
        testCall(db, "RETURN apoc.data.email.domain('foo.foo@bar.baz') AS value",
                row -> Assert.assertThat(row.get("value"), equalTo("bar.baz")));
    }
}
