package apoc;

import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.service.Services;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * NOTE: this is a GLOBAL component, so only once per DBMS
 */
public class ExtendedRegisterComponentFactory extends ExtensionFactory<ExtendedRegisterComponentFactory.Dependencies> {

    private Log log;
    private GlobalProcedures globalProceduresRegistry;

    public ExtendedRegisterComponentFactory() {
        super(ExtensionType.GLOBAL,
                "ApocRegisterComponentExtended");
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

    public class RegisterComponentLifecycle extends LifecycleAdapter {

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

            for (ExtendedApocGlobalComponents c: Services.loadAll(ExtendedApocGlobalComponents.class)) {
                for (Class clazz: c.getContextClasses()) {
                    resolvers.put(clazz, new ConcurrentHashMap<>());
                }
            }

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

    }
}
