package apoc;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.internal.LogService;

/**
 * a kernel extension for the TTL config
 */
public class TTLConfigExtensionFactory extends ExtensionFactory<TTLConfigExtensionFactory.Dependencies> {

    public interface Dependencies {
        ApocConfig config();
        GlobalProcedures globalProceduresRegistry();
    }

    public TTLConfigExtensionFactory() {
        super(ExtensionType.DATABASE, "TTLConfig");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        return new TTLConfig(dependencies.config(), dependencies.globalProceduresRegistry());
    }

}
