package org.neo4j.kernel;

import apoc.result.WeightedNodeResult;
import apoc.result.WeightedRelationshipResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author AgileLARUS
 *
 * @since 21-04-2017
 */
public class KernelApi {

    /*public static ExplicitIndexHits nodeQueryIndex(String indexName, Object query, GraphDatabaseService db) throws Exception {
        return getReadOperation(db).nodeExplicitIndexQuery(indexName, query);
    }
    
    public static ExplicitIndexHits relationshipQueryIndex(String indexName, Object query, GraphDatabaseService db, Long startNode, Long endNode) throws Exception {
        long startingNode = (startNode == null) ? -1 : startNode;
        long endingNode = (endNode == null) ? -1 : endNode;
        return getReadOperation(db).relationshipExplicitIndexQuery(indexName, query, startingNode, endingNode);
    }*/

    public static Node getEndNode(GraphDatabaseService db, long id) {
        Relationship rel = db.getRelationshipById(id);
        return rel.getEndNode();
    }

    /*public static Map<String, String> getIndexConfiguration(String indexName, GraphDatabaseService db) {
        Map<String, String> stringStringMap = null;
        try {
            stringStringMap = getReadOperation(db).nodeExplicitIndexGetConfiguration(indexName);
        } catch (ExplicitIndexNotFoundKernelException e) {
            throw new RuntimeException();
        }
        return stringStringMap;
    }*/

    /*public static List<WeightedNodeResult> toWeightedNodeResultFromExplicitIndex(ExplicitIndexHits hits, GraphDatabaseService db){
        List<WeightedNodeResult> result = new ArrayList<>(hits.size());
        while(hits.hasNext()){
            result.add(new WeightedNodeResult(db.getNodeById(hits.next()), hits.currentScore()));
        }
        return result;
    }*/

    /*public static List<WeightedRelationshipResult> toWeightedRelationshipResultFromExplicitIndex(ExplicitIndexHits hits, GraphDatabaseService db){
        List<WeightedRelationshipResult> result = new ArrayList<>(hits.size());
        while(hits.hasNext()){
            result.add(new WeightedRelationshipResult(db.getRelationshipById(hits.next()), hits.currentScore()));
        }
        return result;
    }*/

    /*private static ReadOperations getReadOperation(GraphDatabaseService db){
        return  ((GraphDatabaseAPI)db)
                .getDependencyResolver()
                .resolveDependency(ThreadToStatementContextBridge.class).get()
                .readOperations();
    }*/
}
