package apoc.neo4j.docker;


import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.Util;
import org.junit.*;
import org.neo4j.driver.Session;
import org.neo4j.graphdb.Result;

import java.util.List;
import java.util.Map;

import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.ExtendedTestContainerUtil.singleResultFirstColumn;

public class SimilarityEntepriseTest {
    
//    private static List nodes = null;

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeAll() throws InterruptedException {
        neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.EXTENDED), true)
                .withNeo4jConfig("apoc.import.file.enabled", "true")
                .withNeo4jConfig("metrics.enabled", "true")
                .withNeo4jConfig("metrics.csv.interval", "1s")
                .withNeo4jConfig("dbms.memory.transaction.total.max", "1G")
                .withNeo4jConfig("server.memory.heap.initial_size", "1G")
                .withNeo4jConfig("server.memory.heap.max_size", "1G")
                .withNeo4jConfig("server.memory.heap.max_size", "1G")
                .withNeo4jConfig("internal.cypher.enable_vector_type", "true")
                .withNeo4jConfig("metrics.namespaces.enabled", "true");
        neo4jContainer.start();
        session = neo4jContainer.getSession();

        session.executeWrite(tx -> tx.run(
    "CYPHER 25 UNWIND range(0, 50000) as id " +
            "CREATE (:Similar {vect: VECTOR([1, 2, id], 3, INTEGER32), id: 1, test: 1}), (:Similar {vect: VECTOR([1, id, 3], 3, INTEGER32), id: 2}), (:Similar {id: 3}), (:Similar {vect: VECTOR([3, 2, id], 3, INTEGER32), ajeje: 1, id: 4}), (:Similar {vect: VECTOR([4, 2, id], 3, INTEGER32), brazorf: 1, id: 5})"
                ).consume()
        );

        // todo - i can't use it: Struct tag: 0x56 representing type VECTOR is not supported for this protocol version
//        nodes = singleResultFirstColumn(session, "MATCH (n:Similar) RETURN collect(n) AS nodes");

//        try (Transaction tx = db.beginTx()) {
//            tx.findNodes(Label.label("Similar")).forEachRemaining(i -> {
//                i.setProperty("embedding", new float[]{1, 2, 4});
//            });
//            tx.commit();
//        }
    }

    @AfterClass
    public static void afterAll() {
        neo4jContainer.close();
    }

    @Test
    public void testSimilarityCompare() {
        long before = System.currentTimeMillis();
        String s = session.executeRead(tx -> tx.run(
                "CYPHER 25 MATCH (node:Similar) WITH COLLECT(node) AS nodes " +
                        "CALL custom.search.batchedSimilarity(nodes, 'vect', VECTOR([1, 2, 3], 3, INTEGER32), 5, 0.8) YIELD node, score " +
                        "RETURN node.id, score",
//                "CALL custom.search.batchedSimilarity($nodes, 'null', null, 5, 0.8, {stopWhenFound: true}) YIELD node, score RETURN node, score",
                Map.of(/*"nodes", nodes*/)).list().toString());
        long after = System.currentTimeMillis();
        System.out.println("after - before apoc proc= " + (after - before));

        System.out.println("s = " + s);
    }

    // TODO - maybe this part: https://neo4j.com/docs/genai/tutorials/embeddings-vector-indexes/embeddings/compute-similarity/
    //   runs faster..
    
    // TODO - https://neo4j.com/docs/genai/tutorials/embeddings-vector-indexes/embeddings/compute-similarity/
    //  it seems the Public APIs still need to be written.
    //  Maybe once they are written, it will be possible to operate with the Java Vector API and SIMD??

    // todo - also cypher with float array
    // todo - I have this warning: WARNING: Java vector incubator module is not readable. For optimal vector performance, pass '--add-modules jdk.incubator.vector' to enable Vector API.
    // @Ignore
    @Test
    public void testSimilarityWithPureCypherInBatch() {
        
        long before = System.currentTimeMillis();
        String cypherRes = session.executeRead(tx -> tx.run("""
                CYPHER 25
                MATCH (node:Similar)
                WITH COLLECT(node) AS nodes
                
                UNWIND nodes AS node
                WITH node, vector.similarity.cosine(node.vect, VECTOR([1, 2, 3], 3, INTEGER32)) AS score
                WHERE score >= $threshold
                RETURN node.id, score
                ORDER BY score DESC
                LIMIT $topK
                """, Map.of(/*"nodes", nodes, */"threshold", 0.8, "queryVector", new float[]{1, 2, 3}, "topK", 5)).list().toString());
        long after = System.currentTimeMillis();
        System.out.println("after - before pure cypher = " + (after - before));
        System.out.println("cypherRes = " + cypherRes);
    }

    // todo - remove
    @Ignore
    @Test
    public void testSimilarity() {
        long before = System.currentTimeMillis();
        String s = session.executeRead(tx -> tx.run(
                "MATCH (n:Similar) WITH collect(n) AS nodes CALL custom.search.batchedSimilarity(nodes, 'null', null, 2, 0.8) YIELD node, score RETURN node, score",
                Map.of()).list().toString());
        long after = System.currentTimeMillis();
        System.out.println("after - before = " + (after - before));

        System.out.println("s = " + s);


        String s1 = session.executeRead(tx -> tx.run(
                "MATCH (n:Similar) WITH collect(n) AS nodes CALL custom.search.batchedSimilarity(nodes, 'null', null, 2, 0.95) YIELD node, score RETURN node, score",
                Map.of()).list().toString());

        System.out.println("s = " + s1);
        String s2 = session.executeRead(tx -> tx.run(
                "MATCH (n:Similar) WITH collect(n) AS nodes CALL custom.search.batchedSimilarity(nodes, 'null', null, 5, 0.8) YIELD node, score RETURN node, score",
                Map.of()).list().toString());

        System.out.println("s = " + s2);


        String s12 = session.executeRead(tx -> tx.run(
                "MATCH (n:Similar) WITH collect(n) AS nodes CALL custom.search.batchedSimilarity(nodes, 'null', null, 5, 0.95) YIELD node, score RETURN node, score",
                Map.of()).list().toString());

        System.out.println("s = " + s12);
    }

    // todo - remove
    @Ignore
    @Test
    public void testSimilarityWithStopWhenFound() {
        String s = session.executeRead(tx -> tx.run(
                "MATCH (n:Similar) WITH collect(n) AS nodes CALL custom.search.batchedSimilarity(nodes, 'null', null, 2, 0.8, {stopWhenFound: true}) YIELD node, score RETURN node, score",
                Map.of()).list().toString());

        System.out.println("stopWhenFound = " + s);
    }

    // todo - remove
    //@Ignore
    @Test
    @Ignore
    public void testSimilarityWithPureCypher() {
//        try (Transaction tx = db.beginTx()) {
//            tx.findNodes(Label.label("Similar")).forEachRemaining(i -> {
//                i.setProperty("embedding", new float[]{1, 2, 4});
//            });
//            tx.commit();
//        }

        long before = System.currentTimeMillis();
        String cypherRes = session.executeRead(tx -> tx.run("""
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
                """, Map.of("threshold", 0.8, "queryVector", new float[]{1, 2, 3}, "topK", 5)).list().toString());
        System.out.println("cypherRes = " + cypherRes);
        long after = System.currentTimeMillis();
        System.out.println("after - before cypher match = " + (after - before));
    }


    // todo - pure cypher with float vector
    // todo - pure cypher with float vector
    // todo - pure cypher with float vector
    
}
