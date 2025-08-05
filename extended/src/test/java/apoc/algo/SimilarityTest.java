package apoc.algo;


import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.neo4j.values.storable.Values;

import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.singleResultFirstColumn;

// TODO
// TODO
// TODO - MOVE TO EXTENDED-IT DUE TO ENTERPRISE VECTOR TYPES
// TODO
public class SimilarityTest {
    
    private static List nodes = null;
    
    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, Similarity.class);
        db.executeTransactionally(
                "UNWIND range(0, 50000) as id CREATE (:Similar {id: 1, test: 1}), (:Similar {id: 2}), (:Similar {id: 3}), (:Similar {ajeje: 1, id: 4}), (:Similar {brazorf: 1, id: 5})");

        nodes = singleResultFirstColumn(db, "MATCH (n:Similar) RETURN collect(n) AS nodes");

        try (Transaction tx = db.beginTx()) {
            tx.findNodes(Label.label("Similar")).forEachRemaining(i -> {
                i.setProperty("embedding", new float[]{1, 2, 4});
            });
            tx.commit();
        }
    }

    @Test
    public void testSimilarityCompare() {
        long before = System.currentTimeMillis();
        String s = db.executeTransactionally(
                "CALL custom.search.batchedSimilarity($nodes, 'null', null, 5, 0.8) YIELD node, score RETURN node, score",
//                "CALL custom.search.batchedSimilarity($nodes, 'null', null, 5, 0.8, {stopWhenFound: true}) YIELD node, score RETURN node, score",
                Map.of("nodes", nodes), Result::resultAsString);
        long after = System.currentTimeMillis();
        System.out.println("after - before = " + (after - before));

        System.out.println("s = " + s);
    }
    
    // TODO - forse questa parte: https://neo4j.com/docs/genai/tutorials/embeddings-vector-indexes/embeddings/compute-similarity/
    //   va più veloce..

    // TODO - https://neo4j.com/docs/genai/tutorials/embeddings-vector-indexes/embeddings/compute-similarity/
    //  pare si debbano ancora scrivere le PublicAPI
    //  forse una volta che vengono scritte si potrà operare con Java Vector API e SIMD??

    // todo - pure cypher with float array
    // todo - ho questo warning: WARNING: Java vector incubator module is not readable. For optimal vector performance, pass '--add-modules jdk.incubator.vector' to enable Vector API.
    @Test
    public void testSimilarityWithPureCypherInBatch() {
        
        long before = System.currentTimeMillis();
        String cypherRes = db.executeTransactionally("""
                //MATCH (node:Similar)
                UNWIND $nodes AS node
                // 2. Calcola la similarità per ogni nodo
                WITH node, vector.similarity.cosine(node.embedding, $queryVector) AS score
                // 3. Filtra i risultati che superano la soglia
                WHERE score >= $threshold
                // 4. Restituisce il nodo e il punteggio, ordinando per trovare i migliori K
                RETURN node, score
                ORDER BY score DESC
                LIMIT $topK
                """, Map.of( "nodes", Util.rebind(nodes, db.beginTx()), "threshold", 0.8, "queryVector", new float[]{1, 2, 3}, "topK", 5), Result::resultAsString);
        long after = System.currentTimeMillis();
        System.out.println("after - before cypher = " + (after - before));
        System.out.println("cypherRes = " + cypherRes);
    }

    // todo - remove
    @Ignore
    @Test
    public void testSimilarity() {
        long before = System.currentTimeMillis();
        String s = db.executeTransactionally(
                "MATCH (n:Similar) WITH collect(n) AS nodes CALL custom.search.batchedSimilarity(nodes, 'null', null, 2, 0.8) YIELD node, score RETURN node, score",
                Map.of(), Result::resultAsString);
        long after = System.currentTimeMillis();
        System.out.println("after - before = " + (after - before));

        System.out.println("s = " + s);


        String s1 = db.executeTransactionally(
                "MATCH (n:Similar) WITH collect(n) AS nodes CALL custom.search.batchedSimilarity(nodes, 'null', null, 2, 0.95) YIELD node, score RETURN node, score",
                Map.of(), Result::resultAsString);

        System.out.println("s = " + s1);
        String s2 = db.executeTransactionally(
                "MATCH (n:Similar) WITH collect(n) AS nodes CALL custom.search.batchedSimilarity(nodes, 'null', null, 5, 0.8) YIELD node, score RETURN node, score",
                Map.of(), Result::resultAsString);

        System.out.println("s = " + s2);


        String s12 = db.executeTransactionally(
                "MATCH (n:Similar) WITH collect(n) AS nodes CALL custom.search.batchedSimilarity(nodes, 'null', null, 5, 0.95) YIELD node, score RETURN node, score",
                Map.of(), Result::resultAsString);

        System.out.println("s = " + s12);
    }

    // todo - remove
    @Ignore
    @Test
    public void testSimilarityWithStopWhenFound() {
        String s = db.executeTransactionally(
                "MATCH (n:Similar) WITH collect(n) AS nodes CALL custom.search.batchedSimilarity(nodes, 'null', null, 2, 0.8, {stopWhenFound: true}) YIELD node, score RETURN node, score",
                Map.of(), Result::resultAsString);

        System.out.println("stopWhenFound = " + s);
    }

    // todo - remove
    @Ignore
    @Test
    public void testSimilarityWithPureCypher() {
//        try (Transaction tx = db.beginTx()) {
//            tx.findNodes(Label.label("Similar")).forEachRemaining(i -> {
//                i.setProperty("embedding", new float[]{1, 2, 4});
//            });
//            tx.commit();
//        }

        long before = System.currentTimeMillis();
        String cypherRes = db.executeTransactionally("""
                MATCH (node:Similar)
                // UNWIND $nodes AS node
                // 2. Calcola la similarità per ogni nodo
                WITH node, vector.similarity.cosine(node.embedding, $queryVector) AS score
                // 3. Filtra i risultati che superano la soglia
                WHERE score >= $threshold
                // 4. Restituisce il nodo e il punteggio, ordinando per trovare i migliori K
                RETURN node, score
                ORDER BY score DESC
                LIMIT $topK
                """, Map.of("threshold", 0.8, "queryVector", new float[]{1, 2, 3}, "topK", 5), Result::resultAsString);
        System.out.println("cypherRes = " + cypherRes);
    }


    // todo - pure cypher with float vector
    // todo - pure cypher with float vector
    // todo - pure cypher with float vector




    // todo - mettere queryNodes
    
    
    
}
