package apoc.warmup;

import apoc.Description;
import apoc.result.Empty;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.RelationshipItem;

import java.util.stream.Stream;

/**
 * @author Sascha Peukert
 * @since 06.05.16
 */
public class Warmup {

    @Context
    public GraphDatabaseAPI db;

    public Warmup(GraphDatabaseAPI db) {
        this.db = db;
    }

    public Warmup() {
    }


    @Procedure
    @Description("apoc.warmup.run() - quickly loads all nodes and rels into memory by skipping one page at a time")
    public Stream<Empty> run(){

        int sizeOfDbPage_byte = 8000;
        int sizeOfNodeRecord_byte=15;
        int sizeOfRelationshipRecord_byte = 35;

        // Getting the highest node and rel ids
        StoreAccess storeAccess = new StoreAccess( db.getDependencyResolver()
                .resolveDependency( RecordStorageEngine.class ).testAccessNeoStores() );
        NeoStores neoStore = storeAccess.getRawNeoStores();
        long highestNodeKey = neoStore.getNodeStore().getHighestPossibleIdInUse();
        long highestRelationshipKey = neoStore.getRelationshipStore().getHighestPossibleIdInUse();

        try ( Transaction tx = db.beginTx() )
        {
            ThreadToStatementContextBridge ctx = db.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
            ReadOperations readOperations = ctx.get().readOperations();

            // Load specific nodes and rels
            for(int i=0; i<=highestNodeKey; i=i+(sizeOfDbPage_byte/sizeOfNodeRecord_byte)){
                loadNode(i,readOperations);
            }
            for(int i=0; i<=highestRelationshipKey; i=i+(sizeOfDbPage_byte/sizeOfRelationshipRecord_byte)){
                loadRelationship(i,readOperations);
            }
            tx.success();
        }
        return Empty.stream(false);
    }


    private void loadNode(long id, ReadOperations rOps){
        try{
            Cursor<NodeItem> c = rOps.nodeCursor(id);
            c.next();
            c.close();
        } catch (Exception e){}
    }

    private void loadRelationship(long id, ReadOperations rOps){
        try{
            Cursor<RelationshipItem> c = rOps.relationshipCursor(id);
            c.next();
            c.close();
        } catch (Exception e){}
    }
}
