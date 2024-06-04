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
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static apoc.ExtendedApocConfig.APOC_KAFKA_ENABLED;

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

        Map<String, Lifecycle> serviceMap = new HashMap<>();
        serviceMap.put("ttl", new TTLLifeCycle(dependencies.scheduler(),
                db,
                TTLConfig.ttlConfig(),
                dependencies.log().getUserLog(TTLLifeCycle.class)));

        serviceMap.put("uuid", new UuidHandler(db,
                dependencies.databaseManagementService(),
                dependencies.log().getUserLog(Uuid.class),
                dependencies.apocConfig(),
                dependencies.scheduler(),
                dependencies.pools()));

        serviceMap.put("directory", new LoadDirectoryHandler(db,
                dependencies.log().getUserLog(LoadDirectory.class),
                dependencies.pools()));

        serviceMap.put("cypherProcedures", cypherProcedureHandler);

        if (dependencies.apocConfig().getBoolean(APOC_KAFKA_ENABLED)) {
            try {
                Class<?> kafkaHandlerClass = Class.forName("apoc.kafka.KafkaHandler");
                Lifecycle kafkaHandler = (Lifecycle) kafkaHandlerClass
                        .getConstructor(GraphDatabaseAPI.class, Log.class)
                        .newInstance(db, dependencies.log().getUserLog(kafkaHandlerClass));

                serviceMap.put("kafkaHandler", kafkaHandler);
            } catch (Exception e) {
                dependencies.log().getUserLog(ExtendedApocGlobalComponents.class)
                        .warn("""
                    Cannot find the Kafka extra jar.
                    Please put the apoc-kafka-dependencies-5.x.x-all.jar into plugin folder.
                    See the documentation: https://neo4j.com/labs/apoc/5/overview/apoc.kakfa""");
            }
        }

        return serviceMap;

    }

    @Override
    public Collection<Class> getContextClasses() {
        List<Class> contextClasses = new ArrayList<>(
                Arrays.asList(CypherProceduresHandler.class, UuidHandler.class, LoadDirectoryHandler.class)
        );
        try {
            contextClasses.add(Class.forName("apoc.kafka.KafkaHandler"));
        } catch (ClassNotFoundException ignored) {}
        return contextClasses;
    }

    @Override
    public Iterable<AvailabilityListener> getListeners(GraphDatabaseAPI db, ApocExtensionFactory.Dependencies dependencies) {
        CypherProceduresHandler cypherProceduresHandler = cypherProcedureHandlers.get(db);
        return cypherProceduresHandler==null ? Collections.emptyList() : Collections.singleton(cypherProceduresHandler);
    }
}