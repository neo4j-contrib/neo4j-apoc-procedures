package apoc;

import apoc.custom.CypherProceduresHandler;
import apoc.trigger.TriggerHandler;
import apoc.uuid.UuidHandler;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

public class ExtendedRegisterComponentFactory extends ExtensionFactory<ExtendedRegisterComponentFactory.Dependencies> {

    private Log log;
    private GlobalProcedures globalProceduresRegistry;

    public ExtendedRegisterComponentFactory() {
        super(ExtensionType.GLOBAL,
                "ApocExtendedRegisterComponent");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        globalProceduresRegistry = dependencies.globalProceduresRegistry();
        log = dependencies.log().getUserLog(ExtendedRegisterComponentFactory.class);
        return new RegisterComponentLifecycle();
    }

    public interface Dependencies {
        LogService log();
        GlobalProcedures globalProceduresRegistry();
    }

    public class RegisterComponentLifecycle implements Lifecycle {

        private final Map<Class, Map<String, Object>> resolvers = new ConcurrentHashMap<>();

        public void addResolver(String databaseNamme, Class clazz, Object instance) {
            Map<String, Object> classInstanceMap = resolvers.computeIfAbsent(clazz, s -> new ConcurrentHashMap<>());
            classInstanceMap.put(databaseNamme, instance);
        }

        public Map<Class, Map<String, Object>> getResolvers() {
            return resolvers;
        }

        @Override
        public void init() throws Exception {
            // FIXME: after lifecycle issue has been resolved upstream
            resolvers.put(UuidHandler.class, new ConcurrentHashMap<>());
            resolvers.put(CypherProceduresHandler.class, new ConcurrentHashMap<>());
            resolvers.forEach(
                    (clazz, dbFunctionMap) -> globalProceduresRegistry.registerComponent(clazz, context -> {
                        String databaseName = context.graphDatabaseAPI().databaseName();
                        Object instance = dbFunctionMap.get(databaseName);
                        if (instance == null) {
                            log.warn("couldn't find a instance for clazz %s and database %s", clazz.getName(), databaseName);
                        }
                        return instance;
                    }, true)
            );
        }

        @Override
        public void start() throws Exception {
        }

        @Override
        public void stop() throws Exception {
        }

        @Override
        public void shutdown() throws Exception {
        }
    }
}
