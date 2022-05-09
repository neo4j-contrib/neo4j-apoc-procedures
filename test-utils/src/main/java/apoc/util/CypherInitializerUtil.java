package apoc.util;

import org.neo4j.internal.helpers.Listeners;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.ReflectionUtil;
import org.neo4j.test.rule.DbmsRule;

public class CypherInitializerUtil {

    /**
     * get a reference to CypherInitializer for diagnosis. This needs to use reflection.
     * @return
     */
    public static <T> T getInitializer(String dbName, DbmsRule dbmsRule, Class<T> clazz) {
        GraphDatabaseAPI api = ((GraphDatabaseAPI) (dbmsRule.getManagementService().database(dbName)));
        DatabaseAvailabilityGuard availabilityGuard = (DatabaseAvailabilityGuard) api.getDependencyResolver().resolveDependency(AvailabilityGuard.class);;
        try {
            Listeners<AvailabilityListener> listeners = ReflectionUtil.getPrivateField(availabilityGuard, "listeners", Listeners.class);
            for (AvailabilityListener listener: listeners) {
                if (listener.getClass().isAssignableFrom(clazz)) {
                    return (T) listener;
                }
            }
            throw new IllegalStateException("found no cypher initializer");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
