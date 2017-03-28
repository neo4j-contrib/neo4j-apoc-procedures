package apoc.index;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.test.TestGraphDatabaseFactory;

import static apoc.util.TestUtil.*;

public class IndexUpdateTransactionEventHandlerTest {

    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        GraphDatabaseAPI api = (GraphDatabaseAPI) db;

        Log log = api.getDependencyResolver().resolveDependency(LogService.class).getUserLog(IndexUpdateTransactionEventHandler.class);
        db.registerTransactionEventHandler(new IndexUpdateTransactionEventHandler(api, log, false));
        TestUtil.registerProcedure(db, FreeTextSearch.class);
//        TestUtil.registerProcedure(db, FulltextIndex.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void shouldDeletingIndexedNodesSucceed() {
        // setup
        testCallEmpty(db, "call apoc.index.addAllNodesExtended('search_index',{City:['name']},{autoUpdate:true})", null);

        testCallEmpty(db, "create (c:City{name:\"Made Up City\",url:\"/places/nowhere/made-up-city\"})", null);

        // when
        TestUtil.testCall(db, "match (c:City{name:'Made Up City'}) delete c return count(c) as count", map -> {
            Assert.assertEquals(1l, map.get("count"));
        });

    }
}
