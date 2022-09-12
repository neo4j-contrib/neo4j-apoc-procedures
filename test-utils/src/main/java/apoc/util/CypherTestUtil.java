package apoc.util;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class CypherTestUtil {

    public static void cypherRunWithReturnCommon(Result r, boolean onlyReturn) {
        Map<String, Object> row = r.next();
        assertEquals(0L, row.get("row"));
        Map result = (Map) row.get("result");
        final Node n = (Node) result.get("n");
        assertEquals(List.of(Label.label("Node")), n.getLabels());
        assertEquals(Map.of("id", 1L), n.getAllProperties());
        row = r.next();
        assertEquals(-1L, row.get("row"));
        result = (Map) row.get("result");
        cypherRunWithReturnCommon(result, 1L, 0L, 1L, onlyReturn);
        row = r.next();
        assertEquals(0L, row.get("row"));
        result = (Map) row.get("result");
        final Node start = (Node) result.get("start");
        assertEquals(List.of(Label.label("Start")), start.getLabels());
        assertEquals(Map.of("id", 1L), start.getAllProperties());
        final Node end = (Node) result.get("end");
        assertEquals(List.of(Label.label("End")), end.getLabels());
        assertEquals(Map.of("id", 3L), end.getAllProperties());
        final Relationship rel = (Relationship) result.get("rel");
        assertEquals(RelationshipType.withName("REL"), rel.getType());
        assertEquals(Map.of("id", 2L), rel.getAllProperties());
        row = r.next();
        assertEquals(-1L, row.get("row"));
        result = (Map) row.get("result");
        final long expected4 = 2;
        final long expected5 = 1L;
        final long expected3 = 1L;
        cypherRunWithReturnCommon(result, expected4, expected5, expected3, onlyReturn);
        row = r.next();
        assertEquals(0L, row.get("row"));
        result = (Map) row.get("result");
        Path path = (Path) result.get("path");
        cypherRunWithReturnCommon(path, 1L);
        row = r.next();
        assertEquals(1L, row.get("row"));
        result = (Map) row.get("result");
        path = (Path) result.get("path");
        cypherRunWithReturnCommon(path, 2L);
        row = r.next();
        assertEquals(-1L, row.get("row"));
        result = (Map) row.get("result");
        final long expected6 = 4L;
        final long expected7 = 2L;
        final long expected8 = 2L;
        cypherRunWithReturnCommon(result, expected6, expected7, expected8, onlyReturn);
        row = r.next();
        assertEquals(0L, row.get("row"));
        result = (Map) row.get("result");
        final List<Path> paths = (List<Path>) result.get("paths");
        assertEquals(2, paths.size());
        cypherRunWithReturnCommon(paths.get(0), 1L);
        cypherRunWithReturnCommon(paths.get(1), 2L);
        row = r.next();
        result = (Map) row.get("result");
        assertEquals(-1L, row.get("row"));
        final long expected9 = 4L;
        final long expected10 = 2L;
        final long expected11 = 1L;
        cypherRunWithReturnCommon(result, expected9, expected10, expected11, onlyReturn);
        assertFalse(r.hasNext());
    }

    public static void cypherRunWithReturnCommon(Map result, long expected9, long expected10, long expectedRows, boolean onlyReturn) {
        assertEquals(onlyReturn ? 0 : (int) expected9, result.get("nodesCreated"));
        assertEquals(onlyReturn ? 0 : (int) expected10, result.get("relationshipsCreated"));
        assertEquals(expectedRows, (long) result.get("rows"));
    }

    public static void cypherRunWithReturnCommon(Path path, long idProp) {
        assertEquals(1, path.length());
        Node startNode = path.startNode();
        assertEquals(List.of(Label.label("StartPath")), startNode.getLabels());
        assertEquals(Map.of("id", idProp), startNode.getAllProperties());
        Node endNode = path.endNode();
        assertEquals(List.of(Label.label("EndPath")), endNode.getLabels());
        assertEquals(Map.of("id", idProp), endNode.getAllProperties());
        Relationship next = path.relationships().iterator().next();
        assertEquals(RelationshipType.withName("REL_PATH"), next.getType());
        assertEquals(Map.of("id", idProp), next.getAllProperties());
    }

    public static void datasetRunFileWithReturn(GraphDatabaseService db) {
        db.executeTransactionally("CREATE (n:Node {id:1})");
        db.executeTransactionally("CREATE (:Start {id:1})-[:REL {id: 2}]->(:End {id: 3})");
        db.executeTransactionally("UNWIND range(1,2) as idx WITH idx CREATE path=(:StartPath {id:idx})-[:REL_PATH {id: idx}]->(:EndPath {id: idx})");
    }
}
