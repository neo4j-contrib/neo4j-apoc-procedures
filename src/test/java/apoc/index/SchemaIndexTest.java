package apoc.index;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 23.05.16
 */
public class SchemaIndexTest {

    private static GraphDatabaseService db;

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .newGraphDatabase();
        TestUtil.registerProcedure(db, SchemaIndex.class);
        db.execute("UNWIND range(1,200) as id CREATE (:Person {name:'name'+id, id:id, age:id % 100})").close();
        db.execute("CREATE CONSTRAINT ON (p:Person) ASSERT p.id IS UNIQUE").close();
        db.execute("CREATE INDEX ON :Person(name)").close();
        db.execute("CREATE INDEX ON :Person(age)").close();
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testOrderedRangeNumbers() throws Exception {
        TestUtil.testResult(db, "CALL apoc.index.orderedRange('Person','age',10,30,false,10)", r -> {
            for (long i=10;i<20;i++) {
                assertEquals(i,((Node)r.next().get("node")).getProperty("id"));
            }
            assertEquals(false, r.hasNext());
        });
    }

    @Test
    public void testOrderedRangeNumbersLowerBound() throws Exception {
        TestUtil.testResult(db, "CALL apoc.index.orderedRange('Person','age',10,null,false,5)", r -> {
            for (long i=10;i<15;i++) {
                assertEquals(i,((Node)r.next().get("node")).getProperty("id"));
            }
            assertEquals(false, r.hasNext());
        });
    }
    @Test
    public void testOrderedRangeNumbersUpperBound() throws Exception {
        TestUtil.testResult(db, "CALL apoc.index.orderedRange('Person','age',null,10,false,5)", r -> {
            for (long i=1;i<6;i++) {
                assertEquals(i,((Node)r.next().get("node")).getProperty("id"));
            }
            assertEquals(false, r.hasNext());
        });
    }

    @Test
    public void testOrderedRangeNumbersRangeSmallerSize() throws Exception {
        TestUtil.testResult(db, "CALL apoc.index.orderedRange('Person','age',10,15,false,10)", r -> {
            for (long i=10;i<16;i++) {
                assertEquals(i,((Node)r.next().get("node")).getProperty("id"));
            }
            for (long i=110;i<114;i++) {
                assertEquals(i,((Node)r.next().get("node")).getProperty("id"));
            }
            assertEquals(false, r.hasNext());
        });
    }

    @Test
    public void testOrderedRangeText() throws Exception {
        TestUtil.testResult(db, "CALL apoc.index.orderedRange('Person','name','name10','name30',false,10)", r -> {
            assertEquals(2L, ((Node) r.next().get("node")).getProperty("id"));
            assertEquals(3L, ((Node) r.next().get("node")).getProperty("id"));
            for (long i=10;i<18;i++) {
                assertEquals(i, ((Node) r.next().get("node")).getProperty("id"));
            }
            assertEquals(false, r.hasNext());
        });
    }
    @Test
    public void testOrderByText() throws Exception {
        TestUtil.testResult(db, "CALL apoc.index.orderedByText('Person','name','STARTS WITH','name1',false,10)", r -> {
            assertEquals(1L,((Node)r.next().get("node")).getProperty("id"));
            for (long i=10;i<19;i++) {
                assertEquals(i, ((Node) r.next().get("node")).getProperty("id"));
            }
            assertEquals(false, r.hasNext());
        });
    }
}
