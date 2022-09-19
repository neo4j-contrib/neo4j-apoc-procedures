package apoc;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.internal.LogService;

@ServiceProvider
public class ExtendedApocConfigExtensionFactory extends ExtensionFactory<ExtendedApocConfigExtensionFactory.Dependencies>
{
    public interface Dependencies {
        LogService log();
        GlobalProcedures globalProceduresRegistry();
    }

    public ExtendedApocConfigExtensionFactory() {
        super( ExtensionType.GLOBAL, "ExtendedApocConfig");
    }

    @Override
    public Lifecycle newInstance( ExtensionContext context, Dependencies dependencies )
    {
        return new ExtendedApocConfig(dependencies.log(), dependencies.globalProceduresRegistry());

    }
}