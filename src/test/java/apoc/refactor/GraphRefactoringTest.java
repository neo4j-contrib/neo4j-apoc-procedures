package apoc.refactor;

import apoc.convert.Json;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.*;

/**
 * @author mh
 * @since 25.03.16
 */
public class GraphRefactoringTest {

    private GraphDatabaseService db;
    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, GraphRefactoring.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testCloneNodes() throws Exception {
//        TestUtil.testCall(db,
//                "",
//                (row) -> {
//
//        });
    }

    @Test
    public void testMergeNodes() throws Exception {

    }

    @Test
    public void testChangeType() throws Exception {

    }

    @Test
    public void testRedirectRelationship() throws Exception {

    }
}
