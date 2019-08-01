package apoc;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.SimpleLogService;

import java.io.File;

import static apoc.ApocConfig.SUN_JAVA_COMMAND;
import static org.junit.Assert.assertEquals;

public class ApocConfigTest {

    private ApocConfig cut;

    @Before
    public void setup() {
        LogProvider logProvider = new AssertableLogProvider();
        cut = new ApocConfig(null, null, new SimpleLogService(logProvider));
    }

    @Test(expected = IllegalStateException.class)
    public void testDetermineNeo4jConfFolderShouldFail() {

        System.setProperty(SUN_JAVA_COMMAND, "");
        cut.determineNeo4jConfFolder();
    }

    @Test
    public void testDetermineNeo4jConfFolder() {
        System.setProperty(SUN_JAVA_COMMAND, "com.neo4j.server.enterprise.CommercialEntryPoint --home-dir=/home/stefan/neo4j-enterprise-4.0.0-alpha09mr02 --config-dir=/home/stefan/neo4j-enterprise-4.0.0-alpha09mr02/conf");

        assertEquals("/home/stefan/neo4j-enterprise-4.0.0-alpha09mr02/conf", cut.determineNeo4jConfFolder());
    }

    @Test
    public void testApocConfFileBeingLoaded() throws Exception {
        String confDir = new File(getClass().getClassLoader().getResource("apoc.conf").toURI()).getParent();
        System.setProperty(SUN_JAVA_COMMAND, "com.neo4j.server.enterprise.CommercialEntryPoint --home-dir=/home/stefan/neo4j-enterprise-4.0.0-alpha09mr02 --config-dir=" + confDir);
        cut.start();

        assertEquals("bar", cut.getConfig().getString("foo"));
    }
}
