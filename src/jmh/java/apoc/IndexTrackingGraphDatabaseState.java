package apoc;

import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Collections;
import java.util.Map;

public class IndexTrackingGraphDatabaseState extends GraphDatabaseState {

    @Override
    public Map<String, String> getGraphDatabaseConfig() {
//        return Collections.singletonMap("apoc.autoIndex.enabled", "true");
        return Collections.EMPTY_MAP;
    }

    @Override
    void setupGraphDatabase(GraphDatabaseService graphDatabaseService) {
//            TestUtil.registerProcedure(getGraphDatabaseService(), FreeTextSearch.class);
//            graphDatabaseService.execute("call apoc.index.addAllNodesExtended('person_index',{Person:['name']},{autoUpdate:true})");
//        System.out.println("creating index");

        super.setupGraphDatabase(graphDatabaseService);
    }
}
