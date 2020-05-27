package apoc;

import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.internal.LogService;

public class PoolExtensionFactory extends ExtensionFactory<PoolExtensionFactory.Dependencies> {

    public PoolExtensionFactory() {
        super(ExtensionType.GLOBAL, "APOC_POOLS");
    }

    public interface Dependencies {
        GlobalProcedures globalProceduresRegistry();
        LogService log();
        ApocConfig apocConfig();
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        return new Pools(dependencies.log(), dependencies.globalProceduresRegistry(), dependencies.apocConfig());
    }

}
