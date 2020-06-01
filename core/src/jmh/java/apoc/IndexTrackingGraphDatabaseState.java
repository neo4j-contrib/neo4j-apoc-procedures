package apoc;

import apoc.util.TestUtil;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import java.util.Map;

public class IndexTrackingGraphDatabaseState extends GraphDatabaseState {

    @Override
    public Map<String, String> getGraphDatabaseConfig() {
        return MapUtil.genericMap("apoc.autoIndex.enabled", "true",
                "apoc.autoIndex.configUpdateInterval", "-1");
    }

    @Override
    void setupGraphDatabase(GraphDatabaseService graphDatabaseService) {
        try {
            TestUtil.registerProcedure(getGraphDatabaseService(), FreeTextSearch.class);
            //graphDatabaseService.execute("CALL apoc.index.addAllNodesExtended('person_index',{Person:['name']},{autoUpdate:true})");
        } catch (KernelException e) {
            throw new RuntimeException(e);
        }
    }
}
