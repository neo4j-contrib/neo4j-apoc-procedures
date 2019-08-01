package apoc;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.internal.LogService;

/**
 * a kernel extension for the new apoc configuration mechanism
 */
@ServiceProvider
public class ApocConfigExtensionFactory extends ExtensionFactory<ApocConfigExtensionFactory.Dependencies> {

    public interface Dependencies {
        LogService log();
        Config config();
    }

    public ApocConfigExtensionFactory() {
        super("ApocConfig");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        return new ApocConfig(context, dependencies.config(), dependencies.log());
    }

}
