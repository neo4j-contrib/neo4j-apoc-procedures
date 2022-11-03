package apoc;

import apoc.load.Jdbc;
import apoc.util.Util;
import apoc.util.collection.Iterators;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

@ServiceProvider
public class JdbcRegistererInitFactory extends ExtensionFactory<JdbcRegistererInitFactory.Dependencies> {

    public interface Dependencies {
        ExtendedApocConfig extendedApocConfig();
    }

    public JdbcRegistererInitFactory() {
        super(ExtensionType.GLOBAL, "JdbcDriverRegisterer");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        return new LifecycleAdapter() {
            @Override
            public void init() throws Exception {
                // we need to await initialization of ExtendedApocConfig. Unfortunately Neo4j's internal service loading tooling does *not* honor the order of service loader META-INF/services files.
                Util.newDaemonThread(() -> {
                    ExtendedApocConfig extendedApocConfig = dependencies.extendedApocConfig();
                    while (!extendedApocConfig.isInitialized()) {
                        Util.sleep(10);
                    }
                    Iterators.stream(extendedApocConfig.getKeys("apoc.jdbc"))
                            .filter(k -> k.endsWith("driver"))
                            .forEach( Jdbc::loadDriver );
                }).start();
            }
        };
    }

}
