package apoc.refactor.rename;


import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author AgileLARUS
 *
 * @since 03-04-2017
 */
public class RenameTest {

	@Rule
	public DbmsRule db = new ImpermanentDbmsRule();

	@Before public void setUp() throws Exception {
		TestUtil.registerProcedure(db, Rename.class);
	}

	@Test
	public void testRenameLabelForSomeNodes() throws Exception {
		List<Node> nodes = TestUtil.firstColumn(db, "UNWIND range(0,9) as id CREATE (f:Foo {id: id, name: 'name'+id}) RETURN f");
		testCall(db, "CALL apoc.refactor.rename.label($oldName,$newName, $nodes)",
				map("oldName", "Foo", "newName", "Bar", "nodes", nodes.subList(0,3)), (r) -> {});

		assertEquals(3L, resultNodesMatches("Bar", null));
		assertEquals(7L, resultNodesMatches("Foo", null));
	}

	@Test
	public void testRenameLabel() throws Exception {
		db.executeTransactionally("UNWIND range(0,9) AS id CREATE (f:Foo {id: id})");
		testCall(db, "CALL apoc.refactor.rename.label($oldName,$newName)",
				map("oldName", "Foo", "newName", "Bar"), (r) -> {});

		assertEquals(10L, resultNodesMatches("Bar", null));
		assertEquals(0L, resultNodesMatches("Foo", null));
	}

	@Test
	public void testRenameRelationship() throws Exception {
		db.executeTransactionally("UNWIND range(0,9) AS id CREATE (f:Foo {id: id})-[:KNOWS {id: id}]->(l:Fii {id: id})");
		testCall(db, "CALL apoc.refactor.rename.type($oldType,$newType)",
				map("oldType", "KNOWS", "newType", "LOVES"), (r) -> {});

		assertEquals(10L, resultRelationshipsMatches("LOVES", null));
		assertEquals(0L, resultRelationshipsMatches("KNOWS", null));
	}

	@Test
	public void testRenameTypeForSomeRelationships() throws Exception {
		db.executeTransactionally("UNWIND range(0,9) AS id CREATE (f:Foo {id: id})-[:KNOWS {id: id}]->(l:Fii {id: id})");

		List<Relationship> rels = TestUtil.firstColumn(db, "MATCH (:Foo)-[r:KNOWS]->(:Fii) RETURN r LIMIT 2");
		testCall(db, "CALL apoc.refactor.rename.type($oldType,$newType, $rels)",
				map("oldType", "KNOWS", "newType", "LOVES", "rels", rels), (r) -> {
		});

		assertEquals(2L, resultRelationshipsMatches("LOVES", null));
		assertEquals(8L, resultRelationshipsMatches("KNOWS", null));
	}

	@Test
	public void testRenameNodesProperty() throws Exception {
		List<Node> nodes = TestUtil.firstColumn(db, "UNWIND range(0,9) as id CREATE (f:Foo {id: id, name: 'name'+id}) RETURN f");
		testCall(db, "CALL apoc.refactor.rename.nodeProperty($oldName,$newName)",
				map("oldName", "name", "newName", "surname"), (r) -> {});

		assertEquals(10L, resultNodesMatches(null, "surname"));
		assertEquals(0L, resultNodesMatches(null, "name"));
	}

	@Test
	public void testRenamePropertyForSomeNodes() throws Exception {
		List<Node> nodes = TestUtil.firstColumn(db, "UNWIND range(0,9) as id CREATE (f:Foo {id: id, name: 'name'+id}) RETURN f");
		db.executeTransactionally("Create constraint on (n:Foo) assert n.name is UNIQUE");
		testCall(db, "CALL apoc.refactor.rename.nodeProperty($oldName,$newName,$nodes)",
				map("oldName", "name", "newName", "surname","nodes",nodes.subList(0,3)), (r) -> {});

		assertEquals(3L, resultNodesMatches(null, "surname"));
		assertEquals(7L, resultNodesMatches(null, "name"));
	}

	@Test
	public void testRenameTypeProperty() throws Exception {
		db.executeTransactionally("UNWIND range(0,9) as id CREATE (f:Foo {id: id})-[:KNOWS {name: 'name' +id}]->(:Fii)");
		testCall(db, "CALL apoc.refactor.rename.typeProperty($oldName,$newName)",
				map("oldName", "name", "newName", "surname"), (r) -> {});

		assertEquals(10L, resultRelationshipsMatches(null, "surname"));
		assertEquals(0L, resultRelationshipsMatches(null, "name"));
	}

	@Test
	public void testConcurrency() {
		String query = "UNWIND RANGE(1, 100) AS ID \n" +
				"CREATE (a:Account{ID: ID})\n" +
				"CREATE (a)-[:HAS_VOLUME{bonusPeriod:'202001'}]->(:Volume)\n" +
				"CREATE (a)-[:HAS_VOLUME{bonusPeriod:'201912'}]->(:Volume)\n" +
				"WITH a\n" +
				"MATCH (u:Account{ID: a.ID - 10})\n" +
				"CREATE (a)-[:IN_MARKET]->(u)\n";
		db.executeTransactionally(query);

		String testQuery = "MATCH (a:Account)-[r:HAS_VOLUME]->()\n" +
				"WITH COLLECT(DISTINCT r.bonusPeriod) as ps\n" +
				"UNWIND ps as period\n" +
				"MATCH (a:Account)-[r:HAS_VOLUME{bonusPeriod:period}]->()\n" +
				"WITH COLLECT(r) as rs, period\n" +
				"CALL apoc.refactor.rename.type('HAS_VOLUME','HAS_VOLUME_'+period,rs, {batchSize:2}) YIELD committedOperations, batches, failedBatches, total, errorMessages, batch\n" +
				"RETURN committedOperations, batches, failedBatches, total, errorMessages, batch";
		testResult(db, testQuery, Collections.emptyMap(), (r) -> {
			final Map<String, Object> batch = r.<Map<String, Object>>columnAs("batch").next();
			final Map<String, Object> errors = (Map<String, Object>) batch.get("error");
			assertFalse(errors.isEmpty());
		});

	}

	@Test
	public void testRenamePropertyForSomeRelationship() throws Exception {
		db.executeTransactionally("UNWIND range(0,9) AS id CREATE (f:Foo {id: id})-[:KNOWS {name: 'name' + id}]->(l:Fii {id: id})");
		List<Relationship> rels = TestUtil.firstColumn(db, "MATCH (:Foo)-[r:KNOWS]->(:Fii) RETURN r LIMIT 2");
		testCall(db, "CALL apoc.refactor.rename.typeProperty($oldName,$newName,$rels)",
				map("oldName", "name", "newName", "surname","rels",rels), (r) -> {});

		assertEquals(2L, resultRelationshipsMatches(null, "surname"));
		assertEquals(8L, resultRelationshipsMatches(null, "name"));
	}

	private long resultRelationshipsMatches(String type, String prop){
		String query = type != null ? "MATCH ()-[r:"+type+"]->() RETURN count(r) as countResult" : "match ()-[r]->() where exists (r."+prop+") return count(r) as countResult";
		return TestUtil.singleResultFirstColumn(db, query);
	}

	private long resultNodesMatches(String label, String prop) {
		String query = label != null ? "MATCH (b:"+label+") RETURN count(b) as countResult" : "match (n) where exists (n."+prop+") return count(n) as countResult";
		return TestUtil.singleResultFirstColumn(db, query);
	}

}
