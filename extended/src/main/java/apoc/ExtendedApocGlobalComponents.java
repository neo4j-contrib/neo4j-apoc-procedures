package apoc;

import apoc.custom.CypherProcedures;
import apoc.custom.CypherProceduresHandler;
import apoc.load.LoadDirectory;
import apoc.load.LoadDirectoryHandler;
import apoc.ttl.TTLLifeCycle;
import apoc.uuid.Uuid;
import apoc.uuid.UuidHandler;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ServiceProvider
public class ExtendedApocGlobalComponents implements ApocGlobalComponents {

    private final Map<GraphDatabaseService,CypherProceduresHandler> cypherProcedureHandlers = new ConcurrentHashMap<>();

    @Override
    public Map<String, Lifecycle> getServices(GraphDatabaseAPI db, ApocExtensionFactory.Dependencies dependencies) {


        CypherProceduresHandler cypherProcedureHandler = new CypherProceduresHandler(
                db,
                dependencies.scheduler(),
                dependencies.apocConfig(),
                dependencies.log().getUserLog(CypherProcedures.class),
                dependencies.globalProceduresRegistry()
        );
        cypherProcedureHandlers.put(db, cypherProcedureHandler);

        return MapUtil.genericMap(

                "ttl", new TTLLifeCycle(dependencies.scheduler(), db, dependencies.apocConfig(), dependencies.ttlConfig(), dependencies.log().getUserLog(TTLLifeCycle.class)),

                "uuid", new UuidHandler(db,
                dependencies.databaseManagementService(),
                dependencies.log().getUserLog(Uuid.class),
                dependencies.apocConfig(),
                dependencies.globalProceduresRegistry()),

                "directory", new LoadDirectoryHandler(db,
                        dependencies.log().getUserLog(LoadDirectory.class),
                        dependencies.pools()),

                "cypherProcedures", cypherProcedureHandler
        );
    }

    @Override
    public Collection<Class> getContextClasses() {
        return List.of(CypherProceduresHandler.class, UuidHandler.class, LoadDirectoryHandler.class);
    }

    @Override
    public Iterable<AvailabilityListener> getListeners(GraphDatabaseAPI db, ApocExtensionFactory.Dependencies dependencies) {
        CypherProceduresHandler cypherProceduresHandler = cypherProcedureHandlers.get(db);
        return cypherProceduresHandler==null ? Collections.emptyList() : Collections.singleton(cypherProceduresHandler);
    }
}
