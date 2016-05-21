package apoc;

import apoc.schema.AssertSchemaProcedure;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
/**
 * @author mh
 * @since 14.05.16
 */
public class ApocKernelExtensionFactory extends KernelExtensionFactory<ApocKernelExtensionFactory.Dependencies>{

    public ApocKernelExtensionFactory() {
        super("APOC");
    }

    public interface Dependencies {
        GraphDatabaseAPI graphdatabaseAPI();
        Procedures procedures();
//        Log log();
    }

    @Override
    public Lifecycle newInstance(KernelContext context, Dependencies dependencies) throws Throwable {
        GraphDatabaseAPI db = dependencies.graphdatabaseAPI();
        Log log = null; // dependencies.log();
        return new LifecycleAdapter() {
            @Override
            public void start() throws Throwable {
                ApocConfiguration.initialize(db);
                dependencies.procedures().register(new AssertSchemaProcedure(db, log));
            }
        };
    }
}
