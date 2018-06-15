package apoc.atomic;

import apoc.util.ArrayBackedList;
import apoc.util.TestUtil;
import org.junit.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

/**
 * @author AgileLARUS
 *
 * @since 26-06-17
 */
public class AtomicTest {
	private GraphDatabaseService db;

	@Before public void setUp() throws Exception {
		db = new TestGraphDatabaseFactory().newImpermanentDatabase();
		TestUtil.registerProcedure(db, Atomic.class);
	}

	@After public void tearDown() {
		db.shutdown();
	}

	@Test
	public void testAddLong(){
		db.execute("CREATE (p:Person {name:'Tom',age: 40}) CREATE (c:Person {name:'John',age: 40}) CREATE (a:Person {name:'Anne',age: 22})");
		Node node = (Node) db.execute("MATCH (n:Person {name:'Tom'}) RETURN n;").next().get("n");
		testCall(db, "CALL apoc.atomic.add({node},{property},{value})",map("node",node,"property","age","value",10), (r) -> {});
		assertEquals(50L, db.execute("MATCH (n:Person {name:'Tom'}) RETURN n.age as age;").next().get("age"));
	}

	@Test
	public void testAddLongRelationship(){
		db.execute("CREATE (p:Person {name:'Tom',age: 40}) CREATE (c:Person {name:'John',age: 40}) CREATE (p)-[:KNOWS{since:1965}]->(c)");
		Relationship rel = (Relationship) db.execute("MATCH (n:Person {name:'Tom'})-[r:KNOWS]-(c) RETURN r;").next().get("r");
		testCall(db, "CALL apoc.atomic.add({rel},{property},{value},{times})",map("rel",rel,"property","since","value",10,"times",5), (r) -> {});
		assertEquals(1975L, db.execute("MATCH (n:Person {name:'Tom'})-[r:KNOWS]-(c) RETURN r.since as since;").next().get("since"));
	}

	@Test
	public void testAddDouble(){
		db.execute("CREATE (p:Person {name:'Tom',age: 40}) CREATE (c:Person {name:'John',age: "+new Double(35)+"}) CREATE (a:Person {name:'Anne',age: 22})");
		Node node = (Node) db.execute("MATCH (n:Person {name:'John'}) RETURN n;").next().get("n");
		testCall(db, "CALL apoc.atomic.add({node},{property},{value},{times})",map("node",node,"property","age","value",10,"times",5), (r) -> {});
		assertEquals(new Double(45), db.execute("MATCH (n:Person {name:'John'}) RETURN n.age as age;").next().get("age"));
	}

	@Test
	public void testSubLong(){
		db.execute("CREATE (p:Person {name:'Tom',age: 40}) CREATE (c:Person {name:'John',age: 35}) CREATE (a:Person {name:'Anne',age: 22})");
		Node node = (Node) db.execute("MATCH (n:Person {name:'John'}) RETURN n;").next().get("n");
		testCall(db, "CALL apoc.atomic.subtract({node},{property},{value},{times})",map("node",node,"property","age","value",10,"times",5), (r) -> {});
		assertEquals(25L, db.execute("MATCH (n:Person {name:'John'}) RETURN n.age as age;").next().get("age"));
	}

	@Test
	public void testSubLongRelationship(){
		db.execute("CREATE (p:Person {name:'Tom',age: 40}) CREATE (c:Person {name:'John',age: 40}) CREATE (p)-[:KNOWS{since:1965}]->(c)");
		Relationship rel = (Relationship) db.execute("MATCH (n:Person {name:'Tom'})-[r:KNOWS]-(c) RETURN r;").next().get("r");
		testCall(db, "CALL apoc.atomic.subtract({rel},{property},{value},{times})",map("rel",rel,"property","since","value",10,"times",5), (r) -> {});
		assertEquals(1955L, db.execute("MATCH (n:Person {name:'Tom'})-[r:KNOWS]-(c) RETURN r.since as since;").next().get("since"));
	}

	@Test
	public void testConcat(){
	    db.execute("CREATE (p:Person {name:'Tom',age: 35})");
		Node node = (Node) db.execute("MATCH (n:Person {name:'Tom'}) RETURN n;").next().get("n");
		testCall(db, "CALL apoc.atomic.concat({node},{property},{value},{times})",map("node",node,"property","name","value","asson","times",5), (r) -> {});
		assertEquals(35L, db.execute("MATCH (n:Person {name:'Tomasson'}) RETURN n.age as age;").next().get("age"));
	}

	@Test
	public void testConcatRelationship(){
		db.execute("CREATE (p:Person {name:'Angelo',age: 22}) CREATE (c:Company {name:'Larus'}) CREATE (p)-[:WORKS_FOR{role:\"software dev\"}]->(c)");
		Relationship rel = (Relationship) db.execute("MATCH (n:Person {name:'Angelo'})-[r:WORKS_FOR]-(c) RETURN r;").next().get("r");
		testCall(db, "CALL apoc.atomic.concat({rel},{property},{value},{times})",map("rel",rel,"property","role","value","eloper","times",5), (r) -> {});
		assertEquals("software developer", db.execute("MATCH (n:Person {name:'Angelo'})-[r:WORKS_FOR]-(c) RETURN r.role as role;").next().get("role"));
	}

	@Test
	public void testRemoveArrayValueLong(){
		db.execute("CREATE (p:Person {name:'Tom',age: [40,50,60]})");
		Node node = (Node) db.execute("MATCH (n:Person {name:'Tom'}) return n;").next().get("n");
		testCall(db, "CALL apoc.atomic.remove({node},{property},{position},{times})",map("node",node,"property","age","position",1,"times",5), (r) -> {});
		assertEquals(Arrays.asList(40L, 60L).toArray(), new ArrayBackedList(db.execute("MATCH (n:Person {name:'Tom'}) RETURN n.age as age;").next().get("age")).toArray());
	}

    @Test
    public void testRemoveFirstElementArrayValueLong(){
        db.execute("CREATE (p:Person {name:'Tom',age: [40,50,60]})");
        Node node = (Node) db.execute("MATCH (n:Person {name:'Tom'}) return n;").next().get("n");
        testCall(db, "CALL apoc.atomic.remove({node},{property},{position},{times})",map("node",node,"property","age","position",0,"times",5), (r) -> {});
        assertEquals(Arrays.asList(50L, 60L).toArray(), new ArrayBackedList(db.execute("MATCH (n:Person {name:'Tom'}) RETURN n.age as age;").next().get("age")).toArray());
    }

    @Test
    public void testRemoveLastElementArrayValueLong(){
        db.execute("CREATE (p:Person {name:'Tom',age: [40,50,60]})");
        Node node = (Node) db.execute("MATCH (n:Person {name:'Tom'}) return n;").next().get("n");
        testCall(db, "CALL apoc.atomic.remove({node},{property},{position},{times})",map("node",node,"property","age","position",2,"times",5), (r) -> {});
        assertEquals(Arrays.asList(40L, 50L).toArray(), new ArrayBackedList(db.execute("MATCH (n:Person {name:'Tom'}) RETURN n.age as age;").next().get("age")).toArray());
    }

    @Test
    public void testRemoveLastItemArray(){
        db.execute("CREATE (p:Person {name:'Tom',age: [40]})");
        Node node = (Node) db.execute("MATCH (n:Person {name:'Tom'}) return n;").next().get("n");
        testCall(db, "CALL apoc.atomic.remove({node},{property},{position},{times})",map("node",node,"property","age","position",0,"times",5), (r) -> {});
        assertEquals(Arrays.asList().toArray(), new ArrayBackedList(db.execute("MATCH (n:Person {name:'Tom'}) RETURN n.age as age;").next().get("age")).toArray());
    }

    @Test(expected = RuntimeException.class)
    public void testRemoveOutOfArrayIndex(){
        db.execute("CREATE (p:Person {name:'Tom',age: [40,50,60]})");
        Node node = (Node) db.execute("MATCH (n:Person {name:'Tom'}) return n;").next().get("n");
        testCall(db, "CALL apoc.atomic.remove({node},{property},{position},{times})",map("node",node,"property","age","position",5,"times",5), (r) -> {});
        assertEquals(Arrays.asList(40,50,60).toArray(), new ArrayBackedList(db.execute("MATCH (n:Person {name:'Tom'}) RETURN n.age as age;").next().get("age")).toArray());
    }

    @Test(expected = RuntimeException.class)
    public void testRemoveEmptyArray(){
        db.execute("CREATE (p:Person {name:'Tom',age: []})");
        Node node = (Node) db.execute("MATCH (n:Person {name:'Tom'}) return n;").next().get("n");
        testCall(db, "CALL apoc.atomic.remove({node},{property},{position},{times})",map("node",node,"property","age","position",5,"times",5), (r) -> {});
        assertEquals(Arrays.asList().toArray(), new ArrayBackedList(db.execute("MATCH (n:Person {name:'Tom'}) RETURN n.age as age;").next().get("age")).toArray());
    }

	@Test
	public void testInsertArrayValueLong(){
		db.execute("CREATE (p:Person {name:'Tom',age: 40})");
		Node node = (Node) db.execute("MATCH (n:Person {name:'Tom'}) return n;").next().get("n");
		testCall(db, "CALL apoc.atomic.insert({node},{property},{position},{value},{times})",map("node",node,"property","age","position",2,"value",55L,"times",5), (r) -> {});
		assertEquals(Arrays.asList(40L,55L).toArray(), new ArrayBackedList(db.execute("MATCH (n:Person {name:'Tom'}) RETURN n.age as age;").next().get("age")).toArray());
	}

	@Test
	public void testInsertArrayValueLongRelationship(){
		db.execute("CREATE (p:Person {name:'Tom',age: 40}) CREATE (c:Person {name:'John',age: 40}) CREATE (p)-[:KNOWS{since:[40,50,60]}]->(c)");
		Relationship rel = (Relationship) db.execute("MATCH (n:Person {name:'Tom'})-[r:KNOWS]-(c) RETURN r;").next().get("r");
		testCall(db, "CALL apoc.atomic.insert({rel},{property},{position},{value},{times})",map("rel",rel,"property","since","position",2,"value",55L,"times",5), (r) -> {});
		assertEquals(Arrays.asList(40L, 50L, 55L, 60L).toArray(), new ArrayBackedList(db.execute("MATCH (n:Person {name:'Tom'})-[r:KNOWS]-(c) RETURN r.since as since;").next().get("since")).toArray());
	}

	@Test
	public void testUpdateNode(){
		db.execute("CREATE (p:Person {name:'Tom',salary1: 1800, salary2:1500})");
		Node node = (Node) db.execute("MATCH (n:Person {name:'Tom'}) RETURN n;").next().get("n");
		testCall(db, "CALL apoc.atomic.update({node},{property},{operation},{times})",map("node",node,"property","salary1","operation","n.salary1 + n.salary2","times",5), (r) -> {});
		assertEquals(3300L, db.execute("MATCH (n:Person {name:'Tom'}) RETURN n.salary1 as salary;").next().get("salary"));
	}

	@Test
	public void testUpdateRel(){
		db.execute("CREATE (t:Person {name:'Tom'})-[:KNOWS {forYears:5}]->(m:Person {name:'Mary'})");
		Relationship rel = (Relationship) db.execute("MATCH (t:Person {name:'Tom'})-[r:KNOWS]->(m:Person {name:'Mary'}) RETURN r;").next().get("r");
		testCall(db, "CALL apoc.atomic.update({rel},{property},{operation},{times})",map("rel",rel,"property","forYears","operation","n.forYears *3 + n.forYears","times",5), (r) -> {});
		assertEquals(20L, db.execute("MATCH (t:Person {name:'Tom'})-[r:KNOWS]->(m:Person {name:'Mary'}) RETURN r.forYears as forYears;").next().get("forYears"));
	}

	@Test
	public void testConcurrentAdd() throws Exception {
        db.execute("CREATE (p:Person {name:'Tom',age: 40})");
		Node node = (Node) db.execute("MATCH (n:Person {name:'Tom'}) return n;").next().get("n");
        ExecutorService executorService = Executors.newFixedThreadPool(2);

		Runnable task = () -> {
			db.execute("MATCH (p:Person {name:'Tom'}) WITH p CALL apoc.atomic.add(p,'age',10, 5) YIELD oldValue, newValue RETURN *").next().get("newValue");
		};

		Runnable task2 = () -> {
			db.execute("MATCH (p:Person {name:'Tom'}) WITH p CALL apoc.atomic.add(p,'age',10, 5) YIELD oldValue, newValue RETURN *").next().get("newValue");
		};

		executorService.execute(task);
		executorService.execute(task2);
        executorService.shutdown();
		executorService.awaitTermination(2, TimeUnit.SECONDS);

		assertEquals(60L, db.execute("MATCH (n:Person {name:'Tom'}) RETURN n.age as age;").next().get("age"));
	}

	@Test
	public void testConcurrentSubtract() throws Exception {
        db.execute("CREATE (p:Person {name:'Tom',age: 40})");
		ExecutorService executorService = Executors.newFixedThreadPool(2);

		Runnable task = () -> {
			db.execute("MATCH (p:Person {name:'Tom'}) WITH p CALL apoc.atomic.subtract(p,'age',10, 5) YIELD oldValue, newValue RETURN *").next().get("newValue");
		};

		Runnable task2 = () -> {
			db.execute("MATCH (p:Person {name:'Tom'}) WITH p CALL apoc.atomic.subtract(p,'age',10, 5) YIELD oldValue, newValue RETURN *").next().get("newValue");
		};

		executorService.execute(task);
		executorService.execute(task2);
        executorService.shutdown();
		executorService.awaitTermination(2, TimeUnit.SECONDS);

		assertEquals(20L, db.execute("MATCH (n:Person {name:'Tom'}) RETURN n.age as age;").next().get("age"));
	}

	@Test
	public void testConcurrentConcat() throws Exception {
		db.execute("CREATE (p:Person {name:'Tom',age: 40})");
		Node node = (Node) db.execute("MATCH (n:Person {name:'Tom'}) RETURN n;").next().get("n");
        ExecutorService executorService = Executors.newFixedThreadPool(2);

		Runnable task = () -> {
			db.execute("CALL apoc.atomic.concat({node},{property},{value},{times})", map("node",node,"property","name","value","asson","times",5)).next().get("newValue");
		};

		Runnable task2 = () -> {
			db.execute("CALL apoc.atomic.concat({node},{property},{value},{times})", map("node",node,"property","name","value","s","times",5)).next().get("newValue");
		};

		executorService.execute(task);
		executorService.execute(task2);
        executorService.shutdown();
		executorService.awaitTermination(2, TimeUnit.SECONDS);

		String name = db.execute("MATCH (n:Person) return n.name as name;").next().get("name").toString();
		assertEquals(9, name.length());

	}

	@Test
	public void testConcurrentInsert() throws InterruptedException {
		db.execute("CREATE (p:Person {name:'Tom',age: 40})");
		Node node = (Node) db.execute("MATCH (n:Person {name:'Tom'}) RETURN n;").next().get("n");
		ExecutorService executorService = Executors.newFixedThreadPool(2);

        Runnable task = () -> {
			db.execute("CALL apoc.atomic.insert({node},{property},{position},{value},{times})", map("node",node,"property","age","position",2,"value",41L,"times",5)).next().get("newValue");
		};

		Runnable task2 = () -> {
			db.execute("CALL apoc.atomic.insert({node},{property},{position},{value},{times})", map("node",node,"property","age","position",2,"value",42L,"times",5)).next().get("newValue");
		};

		executorService.execute(task);
		executorService.execute(task2);
        executorService.shutdown();
        executorService.awaitTermination(2, TimeUnit.SECONDS);

		assertEquals(3, new ArrayBackedList(db.execute("MATCH (n:Person {name:'Tom'}) RETURN n.age as age;").next().get("age")).toArray().length);
	}

	@Test
	public void testConcurrentRemove() throws InterruptedException {
        db.execute("CREATE (p:Person {name:'Tom',age: [40,50,60]}) CREATE (c:Person {name:'John',age: 40}) CREATE (a:Person {name:'Anne',age: 22})");
        Node node = (Node) db.execute("MATCH (n:Person {name:'Tom'}) return n;").next().get("n");
		ExecutorService executorService = Executors.newFixedThreadPool(2);

		Runnable task = () -> {
            db.execute("CALL apoc.atomic.remove({node},{property},{position},{times})",map("node",node,"property","age","position",0,"times",5)).next().get("newValue");
        };

        Runnable task2 = () -> {
            db.execute("CALL apoc.atomic.remove({node},{property},{position},{times})",map("node",node,"property","age","position",1,"times",5)).next().get("newValue");
		};

		executorService.execute(task);
		executorService.execute(task2);
        executorService.shutdown();
		executorService.awaitTermination(10, TimeUnit.SECONDS);

        assertEquals(1 , new ArrayBackedList(db.execute("MATCH (n:Person {name:'Tom'}) RETURN n.age as age;").next().get("age")).toArray().length);
	}

    @Test
    public void testConcurrentUpdate() throws Exception {
        db.execute("CREATE (p:Person {name:'Tom',salary1: 100, salary2: 100})");
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        Runnable task = () -> {
            db.execute("MATCH (p:Person {name:'Tom'}) WITH p CALL apoc.atomic.update(p,'salary1','n.salary1 - n.salary2',5) YIELD oldValue, newValue RETURN *").next().get("newValue");
        };

        Runnable task2 = () -> {
            db.execute("MATCH (p:Person {name:'Tom'}) WITH p CALL apoc.atomic.update(p,'salary1','n.salary1 + n.salary2',5) YIELD oldValue, newValue RETURN *").next().get("newValue");
        };

        executorService.execute(task);
        executorService.execute(task2);
        executorService.shutdown();
        executorService.awaitTermination(2, TimeUnit.SECONDS);

        assertEquals(100L, db.execute("MATCH (n:Person {name:'Tom'}) RETURN n.salary1 as salary;").next().get("salary"));
    }
}
