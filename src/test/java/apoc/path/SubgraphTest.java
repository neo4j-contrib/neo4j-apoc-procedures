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
import static org.junit.Assert.fail;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

public class SubgraphTest {

	private static GraphDatabaseService db;
	
	private static Long fullGraphCount;

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
	public void testSubgraphWithMaxDepthShouldContainExpectedNodes() throws Throwable {
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
	public void testSubgraphWithLabelFilterShouldContainExpectedNodes() throws Throwable {
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
	public void testSubgraphWithRelationshipFilterShouldContainExpectedNodes() throws Throwable {
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
	public void testOptionalSubgraphNodesShouldReturnNull() throws Throwable {
		String query =
				"MATCH (k:Person {name: 'Keanu Reeves'}) " +
						"CALL apoc.path.subgraphNodes(k,{labelFilter:'+nonExistent', maxLevel:3, optional:true, filterStartNode:true}) yield node " +
						"return node";
		TestUtil.testResult(db, query, (result) -> {
			assertTrue(result.hasNext());
			Map<String, Object> row = result.next();
			assertEquals(null, row.get("node"));
		});
	}

	@Test
	public void testSubgraphAllShouldContainExpectedNodesAndRels() throws Throwable {
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
	public void testOptionalSubgraphAllWithNoResultsShouldReturnEmptyLists() throws Throwable {
		String query =
				"MATCH (k:Person {name: 'Keanu Reeves'}) " +
						"CALL apoc.path.subgraphAll(k,{labelFilter:'+nonExistent', maxLevel:3, optional:true, filterStartNode:true}) yield nodes, relationships " +
						"return nodes as subgraphNodes, relationships as subgraphRelationships";
		TestUtil.testCall(db, query, (row) -> {
			List<NodeResult> subgraphNodes = (List<NodeResult>) row.get("subgraphNodes");
			List<RelationshipResult> subgraphRelationships = (List<RelationshipResult>) row.get("subgraphRelationships");
			assertEquals(0, subgraphNodes.size());
			assertEquals(0, subgraphRelationships.size());
		});
	}

	@Test
	public void testSubgraphAllWithNoResultsShouldReturnEmptyLists() throws Throwable {
		String query =
				"MATCH (k:Person {name: 'Keanu Reeves'}) " +
						"CALL apoc.path.subgraphAll(k,{labelFilter:'+nonExistent', maxLevel:3, filterStartNode:true}) yield nodes, relationships " +
						"return nodes as subgraphNodes, relationships as subgraphRelationships";
		TestUtil.testCall(db, query, (row) -> {
			List<NodeResult> subgraphNodes = (List<NodeResult>) row.get("subgraphNodes");
			List<RelationshipResult> subgraphRelationships = (List<RelationshipResult>) row.get("subgraphRelationships");
			assertEquals(0, subgraphNodes.size());
			assertEquals(0, subgraphRelationships.size());
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

	@Test
	public void testOptionalSpanningTreeWithNoResultsShouldReturnNull() throws Throwable {
		String query =
				"MATCH (k:Person {name: 'Keanu Reeves'}) " +
						"CALL apoc.path.spanningTree(k,{labelFilter:'+nonExistent', maxLevel:3, optional:true, filterStartNode:true}) yield path " +
						"return path";
		TestUtil.testResult(db, query, (result) -> {
			assertTrue(result.hasNext());
			Map<String, Object> row = result.next();
			assertEquals(null, row.get("path"));
		});
	}

	@Test
	public void testOptionalSubgraphWithResultsShouldYieldExpectedResults() throws Throwable {
		String controlQuery = "MATCH (m:Movie {title: 'The Matrix'})-[*0..3]-(subgraphNode) return collect(distinct subgraphNode) as subgraph";
		List<NodeResult> subgraph;
		try (Transaction tx = db.beginTx()) {
			Result result = db.execute(controlQuery);
			subgraph = (List<NodeResult>) result.next().get("subgraph");
		}

		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.subgraphNodes(m,{maxLevel:3, optional:true}) yield node return COLLECT(node) as subgraphNodes";
		TestUtil.testCall(db, query, (row) -> {
			List<NodeResult> subgraphNodes = (List<NodeResult>) row.get("subgraphNodes");
			assertEquals(subgraph.size(), subgraphNodes.size());
			assertTrue(subgraph.containsAll(subgraphNodes));
		});
	}

	@Test
	public void testSubgraphNodesAllowsMinLevel0() throws Throwable {
		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.subgraphNodes(m,{minLevel:0}) yield node return count(distinct node) as cnt";
		TestUtil.testCall(db, query, (row) -> assertEquals(fullGraphCount, row.get("cnt")));
	}

	@Test
	public void testSubgraphNodesAllowsMinLevel1() throws Throwable {
		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.subgraphNodes(m,{minLevel:1}) yield node return count(distinct node) as cnt";
		TestUtil.testCall(db, query, (row) -> assertEquals(fullGraphCount - 1, row.get("cnt")));
	}

	@Test
	public void testSubgraphNodesErrorsAboveMinLevel1() throws Throwable {
		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.subgraphNodes(m,{minLevel:2}) yield node return count(distinct node) as cnt";
		try {
			db.execute(query);
		} catch(Throwable t) {
			assertTrue(TestUtil.hasCauses(t, IllegalArgumentException.class));
			assertTrue(t.getMessage().contains("minLevel can only be 0 or 1 in subgraphNodes()"));
			return;
		}

		fail();
	}

	@Test
	public void testSubgraphAllAllowsMinLevel0() throws Throwable {
		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.subgraphAll(m,{minLevel:0}) yield nodes return size(nodes) as cnt";
		TestUtil.testCall(db, query, (row) -> assertEquals(fullGraphCount, row.get("cnt")));
	}

	@Test
	public void testSubgraphAllAllowsMinLevel1() throws Throwable {
		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.subgraphAll(m,{minLevel:1}) yield nodes return size(nodes) as cnt";
		TestUtil.testCall(db, query, (row) -> assertEquals(fullGraphCount - 1, row.get("cnt")));
	}

	@Test
	public void testSubgraphAllErrorsAboveMinLevel1() throws Throwable {
		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.subgraphAll(m,{minLevel:2}) yield nodes return size(nodes) as cnt";
		try {
			Result execute = db.execute(query);
			while (execute.hasNext()) execute.next();
			execute.close();
		} catch(Throwable t) {
			assertTrue(TestUtil.hasCauses(t, IllegalArgumentException.class));
			assertTrue(t.getMessage().contains("minLevel can only be 0 or 1 in subgraphAll()"));
			return;
		}

		fail();
	}

	@Test
	public void testSpanningTreeAllowsMinLevel0() throws Throwable {
		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.spanningTree(m,{minLevel:0}) yield path return count(distinct path) as cnt";
		TestUtil.testCall(db, query, (row) -> assertEquals(fullGraphCount, row.get("cnt")));
	}

	@Test
	public void testSpanningTreeAllowsMinLevel1() throws Throwable {
		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.spanningTree(m,{minLevel:1}) yield path return count(distinct path) as cnt";
		TestUtil.testCall(db, query, (row) -> assertEquals(fullGraphCount - 1, row.get("cnt")));
	}

	@Test
	public void testSpanningTreeErrorsAboveMinLevel1() throws Throwable {
		String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.spanningTree(m,{minLevel:2}) yield path return count(distinct path) as cnt";
		try {
			db.execute(query);
		} catch(Throwable t) {
			assertTrue(TestUtil.hasCauses(t, IllegalArgumentException.class));
			assertTrue(t.getMessage().contains("minLevel can only be 0 or 1 in spanningTree()"));
			return;
		}

		fail();
	}
}
