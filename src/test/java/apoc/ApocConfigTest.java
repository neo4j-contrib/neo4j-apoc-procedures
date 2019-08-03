package apoc;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.configuration.Config;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.SimpleLogService;

import java.io.File;
import java.util.Collections;

import static apoc.ApocConfig.SUN_JAVA_COMMAND;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ApocConfigTest {

    private ApocConfig cut;

    @Before
    public void setup() {
        LogProvider logProvider = new AssertableLogProvider();

        Config neo4jConfig = mock(Config.class);
        when(neo4jConfig.getDeclaredSettings()).thenReturn(Collections.emptyMap());
        cut = new ApocConfig(neo4jConfig, new SimpleLogService(logProvider), null);
    }

    @Test
    public void testDetermineNeo4jConfFolderDefault() {
        System.setProperty(SUN_JAVA_COMMAND, "");
        assertEquals(".", cut.determineNeo4jConfFolder());
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
