package apoc.refactor.util;

import apoc.refactor.GraphRefactoring;
import apoc.util.ArrayBackedList;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Arrays;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

/**
 * @author AgileLARUS
 *
 * @since 27-06-17
 */
public class PropertiesManagerTest {

	private GraphDatabaseService db;

	private String QUERY = "MATCH (d:Person {name:'Daniele'})\n" + "MATCH (p:Country {name:'USA'})\n" + "MATCH (d)-[r:TRAVELS_TO]->(p)\n" + "MATCH (d)-[h:GOES_TO]->(p)\n"
					+ "MATCH (d)-[l:FLIGHTS_TO]->(p) return r as rel1,h as rel2";

	@Before
	public void setUp() throws Exception {
		db = new TestGraphDatabaseFactory().newImpermanentDatabase();
		TestUtil.registerProcedure(db, GraphRefactoring.class);
	}

	@After
	public void tearDown() {
		db.shutdown();
	}

	@Test
	public void testCombinePropertiesTargetArrayValuesSourceSingleValuesSameType(){
		long id = db.execute("Create (d:Person {name:'Daniele'})\n" + "Create (p:Country {name:'USA'})\n" + "Create (d)-[:TRAVELS_TO {year:[2010,2015], reason:\"work\"}]->(p)\n"
				+ "Create (d)-[:GOES_TO {year:1995, reason:\"fun\"}]->(p)\n" + "Create (d)-[:FLIGHTS_TO {company:\"Air America\"}]->(p) RETURN id(p) as id ").<Long>columnAs("id").next();
		testCall(db, QUERY , (r) -> {
			Relationship rel1 = (Relationship) r.get("rel1");
			Relationship rel2 = (Relationship) r.get("rel2");

			PropertiesManager.mergeProperties(rel2.getProperties("year"), rel1, "combine");

			assertEquals(Arrays.asList(2010L, 2015L,1995L).toArray(), new ArrayBackedList(rel1.getProperty("year")).toArray());
		});

	}

	@Test
	public void testCombinePropertiesTargetSingleValueSourceArrayValuesSameType(){
		long id = db.execute("Create (d:Person {name:'Daniele'})\n" + "Create (p:Country {name:'USA'})\n" + "Create (d)-[:TRAVELS_TO {year:1995, reason:\"work\"}]->(p)\n"
				+ "Create (d)-[:GOES_TO {year:[2010,2015], reason:\"fun\"}]->(p)\n" + "Create (d)-[:FLIGHTS_TO {company:\"Air America\"}]->(p) RETURN id(p) as id ").<Long>columnAs("id").next();
		testCall(db, QUERY , (r) -> {
					Relationship rel1 = (Relationship) r.get("rel1");
					Relationship rel2 = (Relationship) r.get("rel2");

					PropertiesManager.mergeProperties(rel2.getProperties("year"), rel1, "combine");

					assertEquals(Arrays.asList(1995L,2010L, 2015L).toArray(), new ArrayBackedList(rel1.getProperty("year")).toArray());
				});

	}

	@Test
	public void testCombinePropertiesTargetArrayValueSourceArrayValuesSameType(){
		long id = db.execute("Create (d:Person {name:'Daniele'})\n" + "Create (p:Country {name:'USA'})\n" + "Create (d)-[:TRAVELS_TO {year:[1995,2014], reason:\"work\"}]->(p)\n"
				+ "Create (d)-[:GOES_TO {year:[2010,2015], reason:\"fun\"}]->(p)\n" + "Create (d)-[:FLIGHTS_TO {company:\"Air America\"}]->(p) RETURN id(p) as id ").<Long>columnAs("id").next();
		testCall(db, QUERY , (r) -> {
					Relationship rel1 = (Relationship) r.get("rel1");
					Relationship rel2 = (Relationship) r.get("rel2");

					PropertiesManager.mergeProperties(rel2.getProperties("year"), rel1, "combine");

					assertEquals(Arrays.asList(1995L,2014L,2010L, 2015L).toArray(), new ArrayBackedList(rel1.getProperty("year")).toArray());
				});

	}

	@Test
	public void testCombinePropertiesTargetArrayValuesSourceSingleValuesDifferentType(){
		long id = db.execute("Create (d:Person {name:'Daniele'})\n" + "Create (p:Country {name:'USA'})\n" + "Create (d)-[:TRAVELS_TO {year:[2010,2015], reason:\"work\"}]->(p)\n"
				+ "Create (d)-[:GOES_TO {year:\"1995\", reason:\"fun\"}]->(p)\n" + "Create (d)-[:FLIGHTS_TO {company:\"Air America\"}]->(p) RETURN id(p) as id ").<Long>columnAs("id").next();
		testCall(db, QUERY , (r) -> {
			Relationship rel1 = (Relationship) r.get("rel1");
			Relationship rel2 = (Relationship) r.get("rel2");

			PropertiesManager.mergeProperties(rel2.getProperties("year"), rel1, "combine");

			assertEquals(Arrays.asList("2010", "2015","1995").toArray(), new ArrayBackedList(rel1.getProperty("year")).toArray());
		});

	}

	@Test
	public void testCombinePropertiesTargetSingleValueSourceArrayValuesDifferentType(){
		long id = db.execute("Create (d:Person {name:'Daniele'})\n" + "Create (p:Country {name:'USA'})\n" + "Create (d)-[:TRAVELS_TO {year:1995, reason:\"work\"}]->(p)\n"
				+ "Create (d)-[:GOES_TO {year:[\"2010\",\"2015\"], reason:\"fun\"}]->(p)\n" + "Create (d)-[:FLIGHTS_TO {company:\"Air America\"}]->(p) RETURN id(p) as id ").<Long>columnAs("id").next();
		testCall(db, QUERY , (r) -> {
			Relationship rel1 = (Relationship) r.get("rel1");
			Relationship rel2 = (Relationship) r.get("rel2");

			PropertiesManager.mergeProperties(rel2.getProperties("year"), rel1, "combine");

			assertEquals(Arrays.asList("1995","2010", "2015").toArray(), new ArrayBackedList(rel1.getProperty("year")).toArray());
		});

	}

	@Test
	public void testCombinePropertiesTargetArrayValueSourceArrayValuesDifferentTypeAndOneSameValue(){
		long id = db.execute("Create (d:Person {name:'Daniele'})\n" + "Create (p:Country {name:'USA'})\n" + "Create (d)-[:TRAVELS_TO {year:[\"1995\",\"2014\"], reason:\"work\"}]->(p)\n"
				+ "Create (d)-[:GOES_TO {year:[2010,2015], reason:\"fun\"}]->(p)\n" + "Create (d)-[:FLIGHTS_TO {company:\"Air America\"}]->(p) RETURN id(p) as id ").<Long>columnAs("id").next();
		testCall(db, QUERY , (r) -> {
			Relationship rel1 = (Relationship) r.get("rel1");
			Relationship rel2 = (Relationship) r.get("rel2");

			PropertiesManager.mergeProperties(rel2.getProperties("year"), rel1, "combine");

			assertEquals(Arrays.asList("1995","2014","2010","2015").toArray(), new ArrayBackedList(rel1.getProperty("year")).toArray());
		});

	}

	@Test
	public void testCombinePropertiesTargetSingleValueSourceSingleValuesSameTypeAndSameValue(){
		long id = db.execute("Create (d:Person {name:'Daniele'})\n" + "Create (p:Country {name:'USA'})\n" + "Create (d)-[:TRAVELS_TO {year:1996, reason:\"work\"}]->(p)\n"
				+ "Create (d)-[:GOES_TO {year:1996, reason:\"fun\"}]->(p)\n" + "Create (d)-[:FLIGHTS_TO {company:\"Air America\"}]->(p) RETURN id(p) as id ").<Long>columnAs("id").next();
		testCall(db, QUERY , (r) -> {
			Relationship rel1 = (Relationship) r.get("rel1");
			Relationship rel2 = (Relationship) r.get("rel2");

			PropertiesManager.mergeProperties(rel2.getProperties("year"), rel1, "combine");

			assertEquals(1996L, rel1.getProperty("year"));
		});

	}

	@Test
	public void testCombinePropertiesTargetArrayValueSourceArrayValuesSameTypeOneSameValue(){
		long id = db.execute("Create (d:Person {name:'Daniele'})\n" + "Create (p:Country {name:'USA'})\n" + "Create (d)-[:TRAVELS_TO {year:[1995,2014], reason:\"work\"}]->(p)\n"
				+ "Create (d)-[:GOES_TO {year:[2010,2014], reason:\"fun\"}]->(p)\n" + "Create (d)-[:FLIGHTS_TO {company:\"Air America\"}]->(p) RETURN id(p) as id ").<Long>columnAs("id").next();
		testCall(db, QUERY , (r) -> {
			Relationship rel1 = (Relationship) r.get("rel1");
			Relationship rel2 = (Relationship) r.get("rel2");

			PropertiesManager.mergeProperties(rel2.getProperties("year"), rel1, "combine");

			assertEquals(Arrays.asList(1995L,2014L,2010L).toArray(), new ArrayBackedList(rel1.getProperty("year")).toArray());
		});

	}


}
