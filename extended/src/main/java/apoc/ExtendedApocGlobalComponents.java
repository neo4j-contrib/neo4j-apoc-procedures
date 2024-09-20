package apoc;

import apoc.load.LoadDirectory;
import apoc.load.LoadDirectoryHandler;
import apoc.ttl.TTLLifeCycle;
import apoc.uuid.Uuid;
import apoc.uuid.UuidHandler;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@ServiceProvider
public class ExtendedApocGlobalComponents implements ApocGlobalComponents {


    @Override
    public Map<String, Lifecycle> getServices(GraphDatabaseAPI db, ApocExtensionFactory.Dependencies dependencies) {

        return Map.of(

                "ttl", new TTLLifeCycle(dependencies.scheduler(),
                        db,
                        TTLConfig.ttlConfig(),
                        dependencies.log().getUserLog(TTLLifeCycle.class)),

                "uuid", new UuidHandler(db,
                        dependencies.databaseManagementService(),
                        dependencies.log().getUserLog(Uuid.class),
                        dependencies.apocConfig(),
                        dependencies.scheduler(),
                        dependencies.pools()),

                "directory", new LoadDirectoryHandler(db,
                        dependencies.log().getUserLog(LoadDirectory.class),
                        dependencies.pools())

        );
    }

    @Override
    public Collection<Class> getContextClasses() {
        return List.of(UuidHandler.class, LoadDirectoryHandler.class);
    }

    @Override
    public Iterable<AvailabilityListener> getListeners(GraphDatabaseAPI db, ApocExtensionFactory.Dependencies dependencies) {
        return Collections.emptyList();
    }
}
