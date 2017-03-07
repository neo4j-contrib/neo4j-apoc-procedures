package apoc.path;

import apoc.algo.Cover;
import apoc.result.NodeResult;
import apoc.result.RelationshipResult;
import apoc.util.TestUtil;
import apoc.util.Util;
import java.util.List;
import java.util.Map;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

public class SubgraphTest {

	private static GraphDatabaseService db;
	
	private static Long fullGraphCount;

	public SubgraphTest() throws Exception {
	}

	@BeforeClass
	public static void setUp() throws Exception {
		db = new TestGraphDatabaseFactory().newImpermanentDatabase();
		TestUtil.registerProcedure(db, PathExplorer.class);
		TestUtil.registerProcedure(db, Cover.class);
		String movies = Util.readResourceFile("movies.cypher");
		String bigbrother = "MATCH (per:Person) MERGE (bb:BigBrother {name : 'Big Brother' })  MERGE (bb)-[:FOLLOWS]->(per)";
		try (Transaction tx = db.beginTx()) {
			db.execute(movies);
			db.execute(bigbrother);
			tx.success();
		}
		
		String getCounts = 
			"match (n) \n" +
			"return count(n) as graphCount";
		try (Transaction tx = db.beginTx()) {
			Result result = db.execute(getCounts);
			
			Map<String, Object> row = result.next();
			fullGraphCount = (Long) row.get("graphCount");
		}
	}

	@AfterClass
	public static void tearDown() {
		db.shutdown();
	}

	@Test
	public void testFullSubgraphShouldContainAllNodes() throws Throwable {
		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.subgraphNodes(m,{}) yield node return count(distinct node) as cnt";
		TestUtil.testCall(db, query, (row) -> assertEquals(fullGraphCount, row.get("cnt")));
	}
	
	@Test
	public void testSugbraphWithMaxDepthShouldContainExpectedNodes() throws Throwable {
		String controlQuery = "MATCH (m:Movie {title: 'The Matrix'})-[*0..3]-(subgraphNode) return collect(distinct subgraphNode) as subgraph";
		List<NodeResult> subgraph;
		try (Transaction tx = db.beginTx()) {
			Result result = db.execute(controlQuery);
			subgraph = (List<NodeResult>) result.next().get("subgraph");
		}
		
		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.subgraphNodes(m,{maxLevel:3}) yield node return COLLECT(node) as subgraphNodes";
		TestUtil.testCall(db, query, (row) -> {
			List<NodeResult> subgraphNodes = (List<NodeResult>) row.get("subgraphNodes");
			assertEquals(subgraph.size(), subgraphNodes.size());
			assertTrue(subgraph.containsAll(subgraphNodes));
		});
	}
	
	@Test
	public void testSugbraphWithLabelFilterShouldContainExpectedNodes() throws Throwable {
		String controlQuery = "MATCH path = (:Person {name: 'Keanu Reeves'})-[*0..3]-(subgraphNode) where all(node in nodes(path) where node:Person) return collect(distinct subgraphNode) as subgraph";
		List<NodeResult> subgraph;
		try (Transaction tx = db.beginTx()) {
			Result result = db.execute(controlQuery);
			subgraph = (List<NodeResult>) result.next().get("subgraph");
		}
		
		String query = "MATCH (k:Person {name: 'Keanu Reeves'}) CALL apoc.path.subgraphNodes(k,{maxLevel:3, labelFilter:'+Person'}) yield node return COLLECT(node) as subgraphNodes";
		TestUtil.testCall(db, query, (row) -> {
			List<NodeResult> subgraphNodes = (List<NodeResult>) row.get("subgraphNodes");
			assertEquals(subgraph.size(), subgraphNodes.size());
			assertTrue(subgraph.containsAll(subgraphNodes));
		});
	}
	
	@Test
	public void testSugbraphWithRelationshipFilterShouldContainExpectedNodes() throws Throwable {
		String controlQuery = "MATCH path = (:Person {name: 'Keanu Reeves'})-[:ACTED_IN*0..3]-(subgraphNode) return collect(distinct subgraphNode) as subgraph";
		List<NodeResult> subgraph;
		try (Transaction tx = db.beginTx()) {
			Result result = db.execute(controlQuery);
			subgraph = (List<NodeResult>) result.next().get("subgraph");
		}
		
		String query = "MATCH (k:Person {name: 'Keanu Reeves'}) CALL apoc.path.subgraphNodes(k,{maxLevel:3, relationshipFilter:'ACTED_IN'}) yield node return COLLECT(node) as subgraphNodes";
		TestUtil.testCall(db, query, (row) -> {
			List<NodeResult> subgraphNodes = (List<NodeResult>) row.get("subgraphNodes");
			assertEquals(subgraph.size(), subgraphNodes.size());
			assertTrue(subgraph.containsAll(subgraphNodes));
		});
	}

	@Test
	public void testSugbraphAllShouldContainExpectedNodesAndRels() throws Throwable {
		String controlQuery =
				"MATCH path = (:Person {name: 'Keanu Reeves'})-[*0..3]-(subgraphNode) " +
				"with collect(distinct subgraphNode) as subgraph " +
				"call apoc.algo.cover([node in subgraph | id(node)]) yield rel " +
				"return subgraph, collect(rel) as relationships";
		final List<NodeResult> subgraph;
		final List<RelationshipResult> relationships;
		try (Transaction tx = db.beginTx()) {
			Result result = db.execute(controlQuery);
			Map<String, Object> row = result.next();
			subgraph = (List<NodeResult>) row.get("subgraph");
			relationships = (List<RelationshipResult>) row.get("relationships");
		}

		String query =
				"MATCH (k:Person {name: 'Keanu Reeves'}) " +
				"CALL apoc.path.subgraphAll(k,{maxLevel:3}) yield nodes, relationships " +
				"return nodes as subgraphNodes, relationships as subgraphRelationships";
		TestUtil.testCall(db, query, (row) -> {
			List<NodeResult> subgraphNodes = (List<NodeResult>) row.get("subgraphNodes");
			List<RelationshipResult> subgraphRelationships = (List<RelationshipResult>) row.get("subgraphRelationships");
			assertEquals(subgraph.size(), subgraphNodes.size());
			assertTrue(subgraph.containsAll(subgraphNodes));
			assertEquals(relationships.size(), subgraphRelationships.size());
			assertTrue(relationships.containsAll(subgraphRelationships));
		});
	}

	@Test
	public void testSpanningTreeShouldHaveOnlyOnePathToEachNode() throws Throwable {
		String controlQuery = "MATCH (m:Movie {title: 'The Matrix'})-[*0..4]-(subgraphNode) return collect(distinct subgraphNode) as subgraph";
		List<NodeResult> subgraph;
		try (Transaction tx = db.beginTx()) {
			Result result = db.execute(controlQuery);
			subgraph = (List<NodeResult>) result.next().get("subgraph");
		}

		String query =
				"MATCH (m:Movie {title: 'The Matrix'}) " +
						"CALL apoc.path.spanningTree(m,{maxLevel:4}) yield path " +
						"with collect(path) as paths " +
						"with paths, size(paths) as pathCount " +
						"unwind paths as path " +
						"with pathCount, collect(distinct last(nodes(path))) as subgraphNodes " +
						"return pathCount, subgraphNodes";
		TestUtil.testCall(db, query, (row) -> {
			List<NodeResult> subgraphNodes = (List<NodeResult>) row.get("subgraphNodes");
			long pathCount = (Long) row.get("pathCount");
			assertEquals(subgraph.size(), subgraphNodes.size());
			assertTrue(subgraph.containsAll(subgraphNodes));
			// assert every node has a single path to that node - no cycles
			assertEquals(pathCount, subgraphNodes.size());
		});
	}
}
