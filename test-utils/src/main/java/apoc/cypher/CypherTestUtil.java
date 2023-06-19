package apoc.cypher;

import org.neo4j.driver.Session;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import org.neo4j.internal.helpers.collection.Iterators;

import java.util.List;
import java.util.Map;

import static apoc.util.TestContainerUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class CypherTestUtil {
    public static final String CREATE_RETURNQUERY_NODES = "UNWIND range(0,3) as id \n" +
                                                           "CREATE (n:ReturnQuery {id:id})-[:REL {idRel: id}]->(:Other {idOther: id})";
    
    public static final String CREATE_RESULT_NODES = "UNWIND range(0,3) as id \n" +
                                                      "CREATE (n:Result {id:id})-[:REL {idRel: id}]->(:Other {idOther: id})";
    
    // placed in test-utils because is used by extended as well
    public static String SET_NODE = "MATCH (n:Result)-[:REL]->(:Other)\n" +
                                    "SET n.updated = true\n" +
                                    "RETURN n;\n";

    public static String SET_AND_RETURN_QUERIES = "MATCH (n:Result)-[:REL]->(:Other)\n" +
                                      "SET n.updated = true\n" +
                                      "RETURN n;\n" +
                                      "\n" +
                                      "MATCH (n:Result)-[rel:REL]->(o:Other)\n" +
                                      "SET rel.updated = 1\n" +
                                      "RETURN n, o, collect(rel) AS rels;\n" +
                                      "\n" +
                                      "MATCH (n:Result)-[rel:REL]->(o:Other)\n" +
                                      "SET o.updated = 'true'\n" +
                                      "RETURN collect(n) as nodes, collect(rel) as rels, collect(o) as others;\n";

    public static String SIMPLE_RETURN_QUERIES = "MATCH (n:ReturnQuery) RETURN n";

    public static void testRunProcedureWithSimpleReturnResults(Session session, String query, Map<String, Object> params) {
        session.writeTransaction(tx -> tx.run(CREATE_RETURNQUERY_NODES));
        testResult(session, query, params,
                r -> {
                    // check that all results from the 1st statement are correctly returned
                    Map<String, Object> row = r.next();
                    assertReturnQueryNode(row, 0L);
                    row = r.next();
                    assertReturnQueryNode(row, 1L);
                    row = r.next();
                    assertReturnQueryNode(row, 2L);
                    row = r.next();
                    assertReturnQueryNode(row, 3L);

                    // check `queryStatistics` row
                    row = r.next();
                    assertReadOnlyResult(row);

                    assertFalse(r.hasNext());
                });
    }

    public static void assertReadOnlyResult(Map<String, Object> row) {
        Map result = (Map) row.get("result");
        assertEquals(-1L, row.get("row"));
        assertEquals(0L, (long) result.get("nodesCreated"));
        assertEquals(0L, (long) result.get("propertiesSet"));
    }

    private static void assertReturnQueryNode(Map<String, Object> row, long id) {
        assertEquals(id, row.get("row") );

        Map<String, Node> result = (Map<String, Node>) row.get("result");
        assertEquals(1, result.size());
        assertReturnQueryNode(id, result);
    }

    public static void assertReturnQueryNode(long id, Map<String, Node> result) {
        Node n = result.get("n");
        assertEquals(List.of("ReturnQuery"), Iterables.asList(n.labels()));
        assertEquals(Map.of("id", id), n.asMap());
    }

    // placed in test-utils because is used by extended as well
    public static void testRunProcedureWithSetAndReturnResults(Session session, String query, Map<String, Object> params) {
        session.writeTransaction(tx -> tx.run(CREATE_RESULT_NODES));

        testResult(session, query, params,
                r -> {
                    // check that all results from the 1st statement are correctly returned
                    Map<String, Object> row = r.next();
                    assertRunProcNode(row, 0L);
                    row = r.next();
                    assertRunProcNode(row, 1L);
                    row = r.next();
                    assertRunProcNode(row, 2L);
                    row = r.next();
                    assertRunProcNode(row, 3L);

                    // check `queryStatistics` row
                    row = r.next();
                    assertRunProcStatistics(row);

                    // check that all results from the 2nd statement are correctly returned
                    row = r.next();
                    assertRunProcRel(row, 0L);
                    row = r.next();
                    assertRunProcRel(row, 1L);
                    row = r.next();
                    assertRunProcRel(row, 2L);
                    row = r.next();
                    assertRunProcRel(row, 3L);

                    // check `queryStatistics` row
                    row = r.next();
                    assertRunProcStatistics(row);

                    // check that all results from the 3rd statement are correctly returned
                    row = r.next();
                    assertEquals(0L, row.get("row") );
                    Map<String, Object> result = (Map<String, Object>) row.get("result");
                    assertEquals(3, result.size());
                    List<Relationship> rels = (List<Relationship>) result.get("rels");
                    List<Node> nodes = (List<Node>) result.get("nodes");
                    List<Node> others = (List<Node>) result.get("others");
                    assertEquals(4L, rels.size());
                    assertEquals(4L, nodes.size());
                    assertEquals(4L, others.size());
                    row = r.next();

                    // check `queryStatistics` row
                    assertRunProcStatistics(row);
                    assertFalse(r.hasNext());
                });

        // check that the procedure's SET operations work properly
        testResult(session,
                "MATCH p=(:Result {updated:true})-[:REL {updated: 1}]->(:Other {updated: 'true'}) RETURN *",
                r -> assertEquals(4L, Iterators.count(r))
        );
    }

    private static void assertRunProcStatistics(Map<String, Object> row) {
        Map result = (Map) row.get("result");
        assertEquals(-1L, row.get("row"));
        assertEquals(0L, (long) result.get("nodesCreated"));
        assertEquals(4L, (long) result.get("propertiesSet"));
    }

    private static void assertRunProcNode(Map<String, Object> row, long id) {
        assertEquals(id, row.get("row") );

        Map<String, Node> result = (Map<String, Node>) row.get("result");
        assertEquals(1, result.size());
        assertResultNode(id, result);
    }

    public static void assertResultNode(long id, Map<String, Node> result) {
        Node n = result.get("n");
        assertEquals(List.of("Result"), Iterables.asList(n.labels()));
        assertEquals(Map.of("id", id, "updated", true), n.asMap());
    }

    private static void assertRunProcRel(Map<String, Object> row, long id) {
        assertEquals(id, row.get("row") );

        Map<String, Object> result = (Map<String, Object>) row.get("result");
        assertEquals(3, result.size());
        List<Relationship> n = (List<Relationship>) result.get("rels");
        assertEquals(1L, n.size());
        assertEquals("REL", n.get(0).type());
        assertEquals(Map.of("idRel", id, "updated", 1L), n.get(0).asMap());
    }
}
