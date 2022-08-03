package apoc.refactor.util;

import apoc.refactor.GraphRefactoring;
import apoc.util.ArrayBackedList;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;

import static apoc.util.TestUtil.testCall;
import static apoc.util.Util.map;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

/**
 * @author AgileLARUS
 *
 * @since 27-06-17
 */
public class PropertiesManagerTest {

	@Rule
	public DbmsRule db = new ImpermanentDbmsRule();

	private String QUERY = "MATCH (d:Person {name:'Daniele'})\n" + "MATCH (p:Country {name:'USA'})\n" + "MATCH (d)-[r:TRAVELS_TO]->(p)\n" + "MATCH (d)-[h:GOES_TO]->(p)\n"
					+ "MATCH (d)-[l:FLIGHTS_TO]->(p) return r as rel1,h as rel2";

	@Before
	public void setUp() throws Exception {
		TestUtil.registerProcedure(db, GraphRefactoring.class);
	}

	@Test
	public void testCombinePropertiesTargetArrayValuesSourceSingleValuesSameType(){
		long id = TestUtil.singleResultFirstColumn(db, "Create (d:Person {name:'Daniele'})\n" + "Create (p:Country {name:'USA'})\n" + "Create (d)-[:TRAVELS_TO {year:[2010,2015], reason:\"work\"}]->(p)\n"
				+ "Create (d)-[:GOES_TO {year:1995, reason:\"fun\"}]->(p)\n" + "Create (d)-[:FLIGHTS_TO {company:\"Air America\"}]->(p) RETURN id(p) as id ");
		testCall(db, QUERY , (r) -> {
			Relationship rel1 = (Relationship) r.get("rel1");
			Relationship rel2 = (Relationship) r.get("rel2");

			PropertiesManager.mergeProperties(rel2.getProperties("year"), rel1, new RefactorConfig(map("properties","combine")));

			assertEquals(asList(2010L, 2015L,1995L).toArray(), new ArrayBackedList(rel1.getProperty("year")).toArray());
		});

	}

	@Test
	public void testCombinePropertiesTargetSingleValueSourceArrayValuesSameType(){
		long id = TestUtil.singleResultFirstColumn(db, "Create (d:Person {name:'Daniele'})\n" + "Create (p:Country {name:'USA'})\n" + "Create (d)-[:TRAVELS_TO {year:1995, reason:\"work\"}]->(p)\n"
				+ "Create (d)-[:GOES_TO {year:[2010,2015], reason:\"fun\"}]->(p)\n" + "Create (d)-[:FLIGHTS_TO {company:\"Air America\"}]->(p) RETURN id(p) as id ");
		testCall(db, QUERY , (r) -> {
					Relationship rel1 = (Relationship) r.get("rel1");
					Relationship rel2 = (Relationship) r.get("rel2");

					PropertiesManager.mergeProperties(rel2.getProperties("year"), rel1, new RefactorConfig(map("properties","combine")));

					assertEquals(asList(1995L,2010L, 2015L).toArray(), new ArrayBackedList(rel1.getProperty("year")).toArray());
				});

	}

	@Test
	public void testCombinePropertiesTargetArrayValueSourceArrayValuesSameType(){
		db.executeTransactionally("Create (d:Person {name:'Daniele'})\n" + "Create (p:Country {name:'USA'})\n" + "Create (d)-[:TRAVELS_TO {year:[1995,2014], reason:\"work\"}]->(p)\n"
				+ "Create (d)-[:GOES_TO {year:[2010,2015], reason:\"fun\"}]->(p)\n" + "Create (d)-[:FLIGHTS_TO {company:\"Air America\"}]->(p) RETURN id(p) as id ");
		testCall(db, QUERY , (r) -> {
					Relationship rel1 = (Relationship) r.get("rel1");
					Relationship rel2 = (Relationship) r.get("rel2");

					PropertiesManager.mergeProperties(rel2.getProperties("year"), rel1, new RefactorConfig(map("properties","combine")));

					assertEquals(asList(1995L,2014L,2010L, 2015L).toArray(), new ArrayBackedList(rel1.getProperty("year")).toArray());
				});

	}

	@Test
	public void testCombinePropertiesTargetArrayValuesSourceSingleValuesDifferentType(){
		db.executeTransactionally("Create (d:Person {name:'Daniele'})\n" + "Create (p:Country {name:'USA'})\n" + "Create (d)-[:TRAVELS_TO {year:[2010,2015], reason:\"work\"}]->(p)\n"
				+ "Create (d)-[:GOES_TO {year:\"1995\", reason:\"fun\"}]->(p)\n" + "Create (d)-[:FLIGHTS_TO {company:\"Air America\"}]->(p) RETURN id(p) as id ");
		testCall(db, QUERY , (r) -> {
			Relationship rel1 = (Relationship) r.get("rel1");
			Relationship rel2 = (Relationship) r.get("rel2");

			PropertiesManager.mergeProperties(rel2.getProperties("year"), rel1, new RefactorConfig(map("properties","combine")));

			assertEquals(asList("2010", "2015","1995").toArray(), new ArrayBackedList(rel1.getProperty("year")).toArray());
		});

	}

	@Test
	public void testCombinePropertiesTargetSingleValueSourceArrayValuesDifferentType(){
		db.executeTransactionally("Create (d:Person {name:'Daniele'})\n" + "Create (p:Country {name:'USA'})\n" + "Create (d)-[:TRAVELS_TO {year:1995, reason:\"work\"}]->(p)\n"
				+ "Create (d)-[:GOES_TO {year:[\"2010\",\"2015\"], reason:\"fun\"}]->(p)\n" + "Create (d)-[:FLIGHTS_TO {company:\"Air America\"}]->(p) RETURN id(p) as id ");
		testCall(db, QUERY , (r) -> {
			Relationship rel1 = (Relationship) r.get("rel1");
			Relationship rel2 = (Relationship) r.get("rel2");

			PropertiesManager.mergeProperties(rel2.getProperties("year"), rel1, new RefactorConfig(map("properties","combine")));

			assertEquals(asList("1995","2010", "2015").toArray(), new ArrayBackedList(rel1.getProperty("year")).toArray());
		});

	}

	@Test
	public void testCombinePropertiesTargetArrayValueSourceArrayValuesDifferentTypeAndOneSameValue(){
		db.executeTransactionally("Create (d:Person {name:'Daniele'})\n" + "Create (p:Country {name:'USA'})\n" + "Create (d)-[:TRAVELS_TO {year:[\"1995\",\"2014\"], reason:\"work\"}]->(p)\n"
				+ "Create (d)-[:GOES_TO {year:[2010,2015], reason:\"fun\"}]->(p)\n" + "Create (d)-[:FLIGHTS_TO {company:\"Air America\"}]->(p) RETURN id(p) as id ");
		testCall(db, QUERY , (r) -> {
			Relationship rel1 = (Relationship) r.get("rel1");
			Relationship rel2 = (Relationship) r.get("rel2");

			PropertiesManager.mergeProperties(rel2.getProperties("year"), rel1, new RefactorConfig(map("properties","combine")));

			assertEquals(asList("1995","2014","2010","2015").toArray(), new ArrayBackedList(rel1.getProperty("year")).toArray());
		});

	}

	@Test
	public void testCombinePropertiesTargetSingleValueSourceSingleValuesSameTypeAndSameValue(){
		db.executeTransactionally("Create (d:Person {name:'Daniele'})\n" + "Create (p:Country {name:'USA'})\n" + "Create (d)-[:TRAVELS_TO {year:1996, reason:\"work\"}]->(p)\n"
				+ "Create (d)-[:GOES_TO {year:1996, reason:\"fun\"}]->(p)\n" + "Create (d)-[:FLIGHTS_TO {company:\"Air America\"}]->(p) RETURN id(p) as id ");
		testCall(db, QUERY , (r) -> {
			Relationship rel1 = (Relationship) r.get("rel1");
			Relationship rel2 = (Relationship) r.get("rel2");

			PropertiesManager.mergeProperties(rel2.getProperties("year"), rel1, new RefactorConfig(map("properties","combine")));

			assertEquals(1996L, rel1.getProperty("year"));
		});

	}

	@Test
	public void testCombinePropertiesTargetArrayValueSourceArrayValuesSameTypeOneSameValue(){
		db.executeTransactionally("Create (d:Person {name:'Daniele'})\n" + "Create (p:Country {name:'USA'})\n" + "Create (d)-[:TRAVELS_TO {year:[1995,2014], reason:\"work\"}]->(p)\n"
				+ "Create (d)-[:GOES_TO {year:[2010,2014], reason:\"fun\"}]->(p)\n" + "Create (d)-[:FLIGHTS_TO {company:\"Air America\"}]->(p) RETURN id(p) as id ");
		testCall(db, QUERY , (r) -> {
			Relationship rel1 = (Relationship) r.get("rel1");
			Relationship rel2 = (Relationship) r.get("rel2");

			PropertiesManager.mergeProperties(rel2.getProperties("year"), rel1, new RefactorConfig(map("properties","combine")));

			assertEquals(asList(1995L,2014L,2010L).toArray(), new ArrayBackedList(rel1.getProperty("year")).toArray());
		});

	}


	@Test
	public void testMergeProperties() throws Exception {
		List<Node> nodes = TestUtil.firstColumn(db, "UNWIND [{name:'Joe',age:42,kids:'Jane'},{name:'Jane',age:32,kids:'June'}] AS data CREATE (p:Person) SET p = data RETURN p");
		try (Transaction tx = db.beginTx()) {
			Node target = Util.rebind(tx, nodes.get(0));
			Node source = Util.rebind(tx, nodes.get(1));
			PropertiesManager.mergeProperties(source.getAllProperties(), target, new RefactorConfig(
					map("properties",map("nam.*", RefactorConfig.DISCARD, "age", RefactorConfig.OVERWRITE, "kids", RefactorConfig.COMBINE))));
			assertEquals("Joe", target.getProperty("name"));
			assertEquals(32L, target.getProperty("age"));
			assertEquals(asList("Jane","June"), asList((String[])target.getProperty("kids")));
			tx.commit();
		}
	}
}
