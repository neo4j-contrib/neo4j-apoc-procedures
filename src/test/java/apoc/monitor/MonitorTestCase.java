package apoc.monitor;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

public abstract class MonitorTestCase {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        registerProcedure();
    }

    abstract Class procedureClass();

    private void registerProcedure() throws Exception {
        TestUtil.registerProcedure(db, procedureClass());
    }

}
