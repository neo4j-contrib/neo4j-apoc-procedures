package apoc.config;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.ApocSettings.dynamic;
import static org.junit.Assert.assertEquals;
import static org.neo4j.configuration.SettingValueParsers.STRING;

/**
 * @author mh
 * @since 28.10.16
 */
public class ConfigTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(dynamic("foo", STRING), "bar");

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, Config.class);
    }

    @Test
    public void listTest(){
        TestUtil.testCall(db, "CALL apoc.config.list() yield key with * where key STARTS WITH 'foo' RETURN *",(row) -> assertEquals("foo",row.get("key")));
    }

}
