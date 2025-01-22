package apoc;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;

public class ExtendedApocConfigTest {

    private ExtendedApocConfig cut;
    private File apocConfigFile;

    @Before
    public void setup() throws URISyntaxException {
        apocConfigFile =
                new File(getClass().getClassLoader().getResource("apoc.conf").toURI());
        
        Config neo4jConfig = mock(Config.class);
        when(neo4jConfig.getDeclaredSettings()).thenReturn(Collections.emptyMap());
        when(neo4jConfig.get(any())).thenReturn(null);
        when(neo4jConfig.get(GraphDatabaseSettings.allow_file_urls)).thenReturn(false);
        when(neo4jConfig.get(GraphDatabaseSettings.configuration_directory))
                .thenReturn(Path.of(apocConfigFile.getParent()));
        
        InternalLogProvider logProvider = new AssertableLogProvider();
        GlobalProceduresRegistry registry = mock(GlobalProceduresRegistry.class);
        cut = new ExtendedApocConfig(
                neo4jConfig,
                new SimpleLogService(logProvider), 
                registry, 
                "C:/neo4j/neo4j-enterprise-5.x.0/conf");
    }

    @Test
    public void testDetermineNeo4jConfFolderDefault() {
        assertEquals(apocConfigFile.getParent(), cut.determineNeo4jConfFolder());
    }
    @Test
    public void testApocConfFileBeingLoaded() {
        cut.init();

        assertEquals("bar", cut.getConfig().getString("foo"));
    }
}
