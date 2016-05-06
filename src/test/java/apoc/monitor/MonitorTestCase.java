package apoc.monitor;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

public abstract class MonitorTestCase {

    protected static GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        registerProcedure();
    }

    abstract Class procedureClass();

    @After
    public void tearDown() {
        db.shutdown();
    }

    private void registerProcedure() throws Exception {
        TestUtil.registerProcedure(db, procedureClass());
    }

}
