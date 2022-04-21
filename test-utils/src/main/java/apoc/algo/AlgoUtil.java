package apoc.algo;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.helpers.collection.Iterables;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AlgoUtil {
    public static final String SETUP_GEO = "CREATE (b:City {name:'Berlin', coords: point({latitude:52.52464,longitude:13.40514}), lat:52.52464,lon:13.40514})\n" +
            "CREATE (m:City {name:'München', coords: point({latitude:48.1374,longitude:11.5755, height: 1}), lat:48.1374,lon:11.5755})\n" +
            "CREATE (f:City {name:'Frankfurt',coords: point({latitude:50.1167,longitude:8.68333, height: 1}), lat:50.1167,lon:8.68333})\n" +
            "CREATE (h:City {name:'Hamburg', coords: point({latitude:53.554423,longitude:9.994583, height: 1}), lat:53.554423,lon:9.994583})\n" +
            "CREATE (b)-[:DIRECT {dist:255.64*1000}]->(h)\n" +
            "CREATE (b)-[:DIRECT {dist:504.47*1000}]->(m)\n" +
            "CREATE (b)-[:DIRECT {dist:424.12*1000}]->(f)\n" +
            "CREATE (f)-[:DIRECT {dist:304.28*1000}]->(m)\n" +
            "CREATE (f)-[:DIRECT {dist:393.15*1000}]->(h)";


    public static void assertAStarResult(Result r) {
        assertEquals(true, r.hasNext());
        Map<String, Object> row = r.next();
        assertEquals(697, ((Number)row.get("weight")).intValue()/1000) ;
        Path path = (Path) row.get("path");
        assertEquals(2, path.length()) ; // 3nodes, 2 rels
        List<Node> nodes = Iterables.asList(path.nodes());
        assertEquals("München", nodes.get(0).getProperty("name")) ;
        assertEquals("Frankfurt", nodes.get(1).getProperty("name")) ;
        assertEquals("Hamburg", nodes.get(2).getProperty("name")) ;
        assertEquals(false,r.hasNext());
    }
}
