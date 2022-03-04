package apoc;

import apoc.trigger.TriggerHandler;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.internal.Version;
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
public class RegisterComponentFactory extends ExtensionFactory<RegisterComponentFactory.Dependencies> {

    private Log log;
    private GlobalProcedures globalProceduresRegistry;

    public RegisterComponentFactory() {
        super(ExtensionType.GLOBAL,
                "ApocRegisterComponent");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        globalProceduresRegistry = dependencies.globalProceduresRegistry();
        log = dependencies.log().getUserLog(RegisterComponentFactory.class);
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
            final String kernelFullVersion = Version.getKernelVersion();
            final String kernelVersion = getMajorMinVersion(kernelFullVersion);
            final String apocFullVersion = apoc.version.Version.class.getPackage().getImplementationVersion();
            if (kernelVersion != null && !kernelVersion.equals(getMajorMinVersion(apocFullVersion))) {
                log.warn("The apoc version (%s) and the Neo4j version (%s) are incompatible. \n" +
                        "See the compatibility matrix in https://neo4j.com/labs/apoc/4.4/installation/ to see the correct version",
                        apocFullVersion, kernelFullVersion);
            }

            for (ApocGlobalComponents c: Services.loadAll(ApocGlobalComponents.class)) {
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

        private String getMajorMinVersion(String completeVersion) {
            if (completeVersion == null) {
                return null;
            }
            final String[] split = completeVersion.split("\\.");
            return split[0] + "." + split[1];
        }

    }
}
