package apoc;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.internal.LogService;

/**
 * a kernel extension for the new apoc configuration mechanism
 */
public class ApocExtendedInitFactory extends ExtensionFactory<ApocExtendedInitFactory.Dependencies> {

    public interface Dependencies {
        LogService log();
        Config config();
        GlobalProcedures globalProceduresRegistry();
        DatabaseManagementService databaseManagementService();
    }

    public ApocExtendedInitFactory() {
        super(ExtensionType.GLOBAL, "ApocExtendedInit");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        return new ApocExtendedInit(dependencies.config(), dependencies.log(), dependencies.globalProceduresRegistry(), dependencies.databaseManagementService());
    }

}
