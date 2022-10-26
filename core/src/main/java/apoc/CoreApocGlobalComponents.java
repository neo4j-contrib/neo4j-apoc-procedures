package apoc;

import apoc.cypher.CypherInitializer;
import apoc.trigger.TriggerDeprecatedProcsHandler;
import apoc.trigger.TriggerHandlerRead;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@ServiceProvider
public class CoreApocGlobalComponents implements ApocGlobalComponents {

    @Override
    public Map<String,Lifecycle> getServices(GraphDatabaseAPI db, ApocExtensionFactory.Dependencies dependencies) {
        return Map.of("triggerDeprecated", 
                new TriggerDeprecatedProcsHandler(db, 
                        dependencies.databaseManagementService(),
                        dependencies.apocConfig(),
                        dependencies.log().getUserLog(TriggerDeprecatedProcsHandler.class),
                        dependencies.pools(),
                        dependencies.scheduler()),
                "trigger", 
                new TriggerHandlerRead(db, 
                        dependencies.databaseManagementService(),
                        dependencies.log().getUserLog(TriggerHandlerRead.class),
                        dependencies.pools(),
                        dependencies.scheduler())
        );
    }

    @Override
    public Collection<Class> getContextClasses() {
        return List.of(TriggerDeprecatedProcsHandler.class, TriggerHandlerRead.class);
    }

    @Override
    public Iterable<AvailabilityListener> getListeners(GraphDatabaseAPI db, ApocExtensionFactory.Dependencies dependencies) {
        return Collections.singleton(new CypherInitializer(db, dependencies.log().getUserLog(CypherInitializer.class)));
    }
}
