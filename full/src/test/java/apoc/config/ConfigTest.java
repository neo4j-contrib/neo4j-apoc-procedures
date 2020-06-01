package apoc.config;

import apoc.util.TestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 28.10.16
 */
public class ConfigTest {

    @Rule
    public final ProvideSystemProperty systemPropertyRule
            = new ProvideSystemProperty("foo", "bar");

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, Config.class);
    }

    @Test
    public void listTest(){
        TestUtil.testCall(db, "CALL apoc.config.list() yield key with * where key STARTS WITH 'foo' RETURN *",(row) -> Assert.assertEquals("foo",row.get("key")));
    }

}
