package apoc.util;

import apoc.index.FreeTextSearch;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static apoc.util.MapUtil.map;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 19.05.16
 */
public class UtilTest {

    private static GraphDatabaseService db;

    private static Node node;

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Utils.class);
        TestUtil.registerProcedure(db, FreeTextSearch.class);
        try (Transaction tx = db.beginTx()) {
            node = db.createNode(Label.label("User"));
            node.setProperty("name", "foo");
            tx.success();
        }
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testSubMap() throws Exception {
        assertEquals(map("b","c"),Util.subMap(map("a.b","c"),"a"));
        assertEquals(map("b","c"),Util.subMap(map("a.b","c"),"a."));
        assertEquals(map(),Util.subMap(map("a.b","c"),"x"));
    }

    @Test
    public void testPartitionList() throws Exception {
        List list = asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        assertEquals(1,Util.partitionSubList(list,0).count());
        assertEquals(1,Util.partitionSubList(list,1).count());
        assertEquals(2,Util.partitionSubList(list,2).count());
        assertEquals(3,Util.partitionSubList(list,3).count());
        assertEquals(4,Util.partitionSubList(list,4).count());
        assertEquals(5,Util.partitionSubList(list,5).count());
        assertEquals(5,Util.partitionSubList(list,6).count());
        assertEquals(5,Util.partitionSubList(list,7).count());
        assertEquals(5,Util.partitionSubList(list,8).count());
        assertEquals(5,Util.partitionSubList(list,9).count());
        assertEquals(10,Util.partitionSubList(list,10).count());
        assertEquals(10,Util.partitionSubList(list,11).count());
        assertEquals(10,Util.partitionSubList(list,20).count());
    }

    @Test
    public void cleanPassword() throws Exception {
        String url = "http://%slocalhost:7474/path?query#ref";
        assertEquals(format(url,""), Util.cleanUrl(format(url, "user:pass@")));
    }

    @Test
    public void mergeNullMaps() {
        assertNotNull(Util.merge(null, null));
    }
}