package apoc.algo.pagerank;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class NodeCounter
{
    public int getNodeCount( GraphDatabaseService db )
    {
//        Result result = db.execute( "MATCH (n) RETURN max(id(n)) AS maxId" );
//        return ((Number) result.next().get( "maxId" )).intValue() + 1;
        return (int) getNeoStores(db).getNodeStore().getHighestPossibleIdInUse() + 1;
    }

    public NeoStores getNeoStores(GraphDatabaseService db) {
        RecordStorageEngine recordStorageEngine = ((GraphDatabaseAPI)db).getDependencyResolver()
                .resolveDependency(RecordStorageEngine.class);
        StoreAccess storeAccess = new StoreAccess(recordStorageEngine.testAccessNeoStores());
        return storeAccess.getRawNeoStores();
    }

    public int getRelationshipCount( GraphDatabaseService db )
    {
//        Result result = db.execute( "MATCH ()-[r]->() RETURN max(id(r)) AS maxId" );
//        return ((Number) result.next().get( "maxId" )).intValue() + 1;
        return (int) getNeoStores(db).getRelationshipStore().getHighestPossibleIdInUse() + 1;
    }
}
