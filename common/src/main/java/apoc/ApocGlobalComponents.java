package apoc;

import org.neo4j.annotations.service.Service;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;

import java.util.Collection;
import java.util.Map;

@Service
public interface ApocGlobalComponents {
    Map<String,Lifecycle> getServices(GraphDatabaseAPI db, ApocExtensionFactory.Dependencies dependencies);

    Collection<Class> getContextClasses();

    Iterable<AvailabilityListener> getListeners(GraphDatabaseAPI db, ApocExtensionFactory.Dependencies dependencies);
}
