package apoc;

import static apoc.ApocConfig.SUN_JAVA_COMMAND;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.File;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;

public class ExtendedApocConfigTest {

    private ExtendedApocConfig cut;

    @Before
    public void setup() {
        InternalLogProvider logProvider = new AssertableLogProvider();
        GlobalProceduresRegistry registry = mock(GlobalProceduresRegistry.class);
        cut = new ExtendedApocConfig(new SimpleLogService(logProvider), registry);
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
        cut.init();

        assertEquals("bar", cut.getConfig().getString("foo"));
    }

    @Test
    public void testDetermineNeo4jConfFolderWithWhitespaces() {
        System.setProperty(SUN_JAVA_COMMAND, "com.neo4j.server.enterprise.CommercialEntryPoint --config-dir=/home/stefan/neo4j enterprise-4.0.0-alpha09mr02/conf --home-dir=/home/stefan/neo4j enterprise-4.0.0-alpha09mr02");

        assertEquals("/home/stefan/neo4j enterprise-4.0.0-alpha09mr02/conf", cut.determineNeo4jConfFolder());
    }
}
