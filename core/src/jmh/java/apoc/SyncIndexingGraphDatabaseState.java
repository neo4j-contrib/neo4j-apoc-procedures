package apoc;

import org.neo4j.graphdb.GraphDatabaseService;

public class SyncIndexingGraphDatabaseState extends IndexTrackingGraphDatabaseState {

    @Override
    void setupGraphDatabase(GraphDatabaseService graphDatabaseService) {
        super.setupGraphDatabase(graphDatabaseService);
        graphDatabaseService.execute("CALL apoc.index.addAllNodesExtended('person_index',{Person:['name']},{autoUpdate:true})");
    }
}
