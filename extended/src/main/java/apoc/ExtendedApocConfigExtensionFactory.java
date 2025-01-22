package apoc;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.internal.LogService;

import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;

@ServiceProvider
public class ExtendedApocConfigExtensionFactory extends ExtensionFactory<ExtendedApocConfigExtensionFactory.Dependencies>
{
    public interface Dependencies {
        LogService log();
        GlobalProcedures globalProceduresRegistry();

        Config config();
    }

    public ExtendedApocConfigExtensionFactory() {
        super( ExtensionType.GLOBAL, "ExtendedApocConfig");
    }

    @Override
    public Lifecycle newInstance( ExtensionContext context, Dependencies dependencies )
    {
        String defaultConfigPath = dependencies.config()
                .get(neo4j_home)
                .resolve(Config.DEFAULT_CONFIG_DIR_NAME)
                .toString();
        return new ExtendedApocConfig(
                dependencies.config(),
                dependencies.log(), 
                dependencies.globalProceduresRegistry(), 
                defaultConfigPath);

    }
}