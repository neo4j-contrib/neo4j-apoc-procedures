package apoc.algo.pagerank;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;

public class NodeCounter
{
    public int getNodeCount( GraphDatabaseService db )
    {
        Result result = db.execute( "MATCH (n) RETURN max(id(n)) AS maxId" );
        return ((Number) result.next().get( "maxId" )).intValue() + 1;
//        NeoStores neoStore = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(NeoStores.class);
//        return (int) neoStore.getNodeStore().getHighId();
    }

    public int getRelationshipCount( GraphDatabaseService db )
    {
        Result result = db.execute( "MATCH ()-[r]->() RETURN max(id(r)) AS maxId" );
        return ((Number) result.next().get( "maxId" )).intValue() + 1;
//        NeoStores neoStore = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(NeoStores.class);
//        return (int) neoStore.getRelationshipStore().getHighId();
    }
}