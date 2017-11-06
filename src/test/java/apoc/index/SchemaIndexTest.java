package apoc.index;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author mh
 * @since 23.05.16
 */
public class SchemaIndexTest {

    private static GraphDatabaseService db;
    private static List<String> personNames;
    private static List<String> personAddresses;
    private static List<Long> personAges;
    private static List<Long> personIds;
    private static final int firstPerson = 1;
    private static final int lastPerson = 200;

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .newGraphDatabase();
        TestUtil.registerProcedure(db, SchemaIndex.class);
        db.execute("CREATE (city:City {name:'London'}) WITH city UNWIND range("+firstPerson+","+lastPerson+") as id CREATE (:Person {name:'name'+id, id:id, age:id % 100, address:id+'Main St.'})-[:LIVES_IN]->(city)").close();
        db.execute("CREATE INDEX ON :Person(name)").close();
        db.execute("CREATE INDEX ON :Person(age)").close();
        db.execute("CREATE INDEX ON :Person(address)").close();
        db.execute("CREATE CONSTRAINT ON (p:Person) ASSERT p.id IS UNIQUE").close();
        db.execute("CREATE INDEX ON :Foo(bar)").close();
        db.execute("CREATE (f:Foo {bar:'three'}), (f2a:Foo {bar:'four'}), (f2b:Foo {bar:'four'})").close();
        personIds = IntStream.range(firstPerson, lastPerson+1).mapToObj(Long::new).collect(Collectors.toList());
        personNames = IntStream.range(firstPerson, lastPerson+1).mapToObj(Integer::toString).map(i -> "name"+i).sorted().collect(Collectors.toList());
        personAddresses = IntStream.range(firstPerson, lastPerson+1).mapToObj(Integer::toString).map(i -> i+"Main St.").sorted().collect(Collectors.toList());
        personAges = IntStream.range(firstPerson, lastPerson+1).map(i -> i % 100).sorted().mapToObj(Long::new).collect(Collectors.toList());

        try (Transaction tx=db.beginTx()) {
            db.schema().awaitIndexesOnline(2,TimeUnit.SECONDS);
            tx.success();
        }
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testRelated() throws Exception {
        testResult(db, "MATCH (city:City) WITH city CALL apoc.index.related([city],'Person','age','LIVES_IN>',5) YIELD node RETURN *", r -> {
            Function<Result,Object> age = (res) -> ((Node)res.next().get("node")).getProperty("age");
            LongStream.of(0,0,1,1,2).forEach( (a) -> assertEquals(a,age.apply(r)));
            assertEquals(false, r.hasNext());
        });
    }

    @Test
    public void testOrderedRangeNumbers() throws Exception {
        testResult(db, "CALL apoc.index.orderedRange('Person','age',10,30,false,10)", r -> {
            for (long i=10;i<20;i++) {
                Node node = (Node) r.next().get("node");
                long id = (Long) node.getProperty("id");
                long age = (Long) node.getProperty("age");
                assertEquals(id % 100, age);
                assertEquals(true, 10 <= age && age <= 30);
            }
            assertEquals(false, r.hasNext());
        });
    }

    @Test
    public void testOrderedRangeNumbersLowerBound() throws Exception {
        testResult(db, "CALL apoc.index.orderedRange('Person','age',10,null,false,5)", r -> {
            for (long i=10;i<15;i++) {
                Node node = (Node) r.next().get("node");
                long id = (Long) node.getProperty("id");
                long age = (Long) node.getProperty("age");
                assertEquals(id % 100, age);
                assertEquals(true, 10 <= age);
            }
            assertEquals(false, r.hasNext());
        });
    }
    @Test
    public void testOrderedRangeNumbersUpperBound() throws Exception {
        testResult(db, "CALL apoc.index.orderedRange('Person','age',null,10,false,5)", r -> {
            for (long i=1;i<6;i++) {
                Node node = (Node) r.next().get("node");
                long id = (Long) node.getProperty("id");
                long age = (Long) node.getProperty("age");
                assertEquals(id % 100, age);
                assertEquals(true, age <= 10);
            }
            assertEquals(false, r.hasNext());
        });
    }

    @Test
    public void testOrderedRangeNumbersRangeSmallerSize() throws Exception {
        testResult(db, "CALL apoc.index.orderedRange('Person','age',10,15,false,10)", r -> {
            for (long i=0;i<10;i++) {
                Node node = (Node) r.next().get("node");
                long id = (Long) node.getProperty("id");
                long age = (Long) node.getProperty("age");
                assertEquals(id % 100, age);
                assertEquals(true, 10 <= age && age <= 15);
            }
            assertEquals(false, r.hasNext());
        });
    }

    @Test
    public void testOrderedRangeText() throws Exception {
        testResult(db, "CALL apoc.index.orderedRange('Person','name','name10','name30',false,10)", r -> {
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
        testResult(db, "CALL apoc.index.orderedByText('Person','name','STARTS WITH','name1',false,10)", r -> {
            assertEquals(1L,((Node)r.next().get("node")).getProperty("id"));
            for (long i=10;i<19;i++) {
                assertEquals(i, ((Node) r.next().get("node")).getProperty("id"));
            }
            assertEquals(false, r.hasNext());
        });
    }

    @Test
    public void testDistinctPropertiesOnFirstIndex() throws Exception {
        testCall(db,"CALL apoc.schema.properties.distinct({label}, {key})",
                map("label", "Person","key", "name"),
                (row) -> assertEquals(new HashSet<>(personNames), new HashSet<>((Collection<String>) row.get("value")))
        );
    }

    @Test
    public void testDistinctPropertiesOnSecondIndex() throws Exception {
        testCall(db,"CALL apoc.schema.properties.distinct({label}, {key})",
                map("label", "Person","key", "address"),
                (row) -> assertEquals(new HashSet<>(personAddresses), new HashSet<>((Collection<String>) row.get("value")))
        );
    }

    @Test
    public void testDistinctCountPropertiesOnFirstIndex() throws Exception {
        String label = "Person";
        String key = "name";
        testResult(db,"CALL apoc.schema.properties.distinctCount({label}, {key}) YIELD label,key,value,count RETURN * ORDER BY value",
                map("label",label,"key",key),
                (result) -> {
                    assertDistinctCountProperties("Person", "name", personNames, () -> 1L, result);
                    assertFalse(result.hasNext());
        });
    }

    @Test
    public void testDistinctCountPropertiesOnSecondIndex() throws Exception {
        String label = "Person";
        String key = "address";
        testResult(db,"CALL apoc.schema.properties.distinctCount({label}, {key}) YIELD label,key,value,count RETURN * ORDER BY value",
                map("label",label,"key",key),
                (result) -> {
                    assertDistinctCountProperties("Person", "address", personAddresses, () -> 1L, result);
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testDistinctCountPropertiesOnEmptyLabel() throws Exception {
        String key = "name";
        testResult(db,"CALL apoc.schema.properties.distinctCount({label}, {key}) YIELD label,key,value,count RETURN * ORDER BY value",
                map("label","","key",key),
                (result) -> {
                    assertDistinctCountProperties("Person", "name", personNames, () -> 1L, result);
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testDistinctCountPropertiesOnEmptyKey() throws Exception {
        String label = "Person";
        testResult(db,"CALL apoc.schema.properties.distinctCount({label}, {key}) YIELD label,key,value,count RETURN * ORDER BY key,value",
                map("label",label,"key",""),
                (result) -> {
                    assertDistinctCountProperties("Person", "address", personAddresses, () -> 1L, result);
                    assertDistinctCountProperties("Person", "name", personNames, () -> 1L, result);
                    //todo: update when number terms are supported
                    //assertDistinctCountProperties("Person", "id", personIds, () -> 1L, result);
                    //assertDistinctCountProperties("Person", "age", personAges, () -> 1L, result);
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testDistinctCountPropertiesOnEmptyLabelAndEmptyKey() throws Exception {
        testResult(db,"CALL apoc.schema.properties.distinctCount({label}, {key}) YIELD label,key,value,count RETURN * ORDER BY label,key,value",
                map("label","","key",""),
                (result) -> {
                    assertTrue(result.hasNext());
                    assertEquals(map("label","Foo","key","bar","value","four","count",2L),result.next());
                    assertEquals(map("label","Foo","key","bar","value","three","count",1L),result.next());
                    assertDistinctCountProperties("Person", "address", personAddresses, () -> 1L, result);
                    assertDistinctCountProperties("Person", "name", personNames, () -> 1L, result);
                    //todo: update when number terms are supported
                    //assertDistinctCountProperties("Person", "id", personIds, () -> 1L, result);
                    //assertDistinctCountProperties("Person", "age", personAges, () -> 1L, result);
                    assertFalse(result.hasNext());
                });
    }

    private <T> void assertDistinctCountProperties(String label, String key, Collection<T> values, Supplier<Long> counts, Result result) {
        Iterator<T> valueIterator = values.iterator();

        while (valueIterator.hasNext()) {
            assertTrue(result.hasNext());
            Map<String,Object> map = result.next();
            assertEquals(label, map.get("label"));
            assertEquals(key, map.get("key"));
            assertEquals(valueIterator.next(), map.get("value"));
            assertEquals(counts.get(), map.get("count"));
        }
    }
}
