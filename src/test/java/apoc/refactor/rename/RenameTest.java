package apoc.refactor.rename;


import apoc.result.VirtualNode;
import apoc.util.MapUtil;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Arrays;
import java.util.List;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

/**
 * @author AgileLARUS
 *
 * @since 03-04-2017
 */
public class RenameTest {

	private GraphDatabaseService db;

	@Before public void setUp() throws Exception {
		db = new TestGraphDatabaseFactory().newImpermanentDatabase();
		TestUtil.registerProcedure(db, Rename.class);
	}

	@After public void tearDown() {
		db.shutdown();
	}

	@Test
	public void testRenameLabelForSomeNodes() throws Exception {
		for (int i = 0; i < 10; i++)
			db.execute("CREATE (f:Foo {id: {id}})", MapUtil.map("id", i));
		testCall(db, "CALL apoc.refactor.rename.label({oldName},{newName}, {nodes})",
				map("oldName", "Foo", "newName", "Bar", "nodes", mockNodes()), (r) -> {});

		assertEquals(new Long(3), resultNodesMatches("Bar", null));
		assertEquals(new Long(7), resultNodesMatches("Foo", null));
	}

	@Test
	public void testRenameLabel() throws Exception {
		for (int i = 0; i < 10; i++)
			db.execute("CREATE (f:Foo {id: {id}})", MapUtil.map("id", i));
		testCall(db, "CALL apoc.refactor.rename.label({oldName},{newName})",
				map("oldName", "Foo", "newName", "Bar"), (r) -> {});

		assertEquals(new Long(10), resultNodesMatches("Bar", null));
		assertEquals(new Long(0), resultNodesMatches("Foo", null));
	}

	@Test
	public void testRenameRelationship() throws Exception {
		for (int i = 0; i < 10; i++)
			db.execute("CREATE (f:Foo {id: {id}})-[:KNOWS {id: {id}}]->(l:Fii {id: {id}})", MapUtil.map("id", i));
		testCall(db, "CALL apoc.refactor.rename.type({oldType},{newType})",
				map("oldType", "KNOWS", "newType", "LOVES"), (r) -> {});

		assertEquals(new Long(10), resultRelationshipsMatches("LOVES", null));
		assertEquals(new Long(0), resultRelationshipsMatches("KNOWS", null));
	}

	@Test
	public void testRenameTypeForSomeRelationships() throws Exception {
		for (int i = 0; i < 10; i++)
			db.execute("CREATE (f:Foo {id: {id}})-[:KNOWS {id: {id}}]->(l:Fii {id: {id}})", MapUtil.map("id", i));
		Result result = db.execute("MATCH (:Foo)-[r:KNOWS]->(:Fii) RETURN r LIMIT 2");
		testCall(db, "CALL apoc.refactor.rename.type({oldType},{newType}, {rels})",
				map("oldType", "KNOWS", "newType", "LOVES", "rels", mockRelationships(result)), (r) -> {
		});

		assertEquals(new Long(2), resultRelationshipsMatches("LOVES", null));
		assertEquals(new Long(8), resultRelationshipsMatches("KNOWS", null));
	}

	@Test
	public void testRenameNodesProperty() throws Exception {
		for (int i = 0; i < 10; i++)
			db.execute("CREATE (f:Foo {id: {id}, name: {name}})", MapUtil.map("id", i, "name", "name"+i));
		testCall(db, "CALL apoc.refactor.rename.nodeProperty({oldName},{newName})",
				map("oldName", "name", "newName", "surname"), (r) -> {});

		assertEquals(new Long(10), resultNodesMatches(null, "surname"));
		assertEquals(new Long(0), resultNodesMatches(null, "name"));
	}

	@Test
	public void testRenamePropertyForSomeNodes() throws Exception {
		for (int i = 0; i < 10; i++)
			db.execute("CREATE (f:Foo {id: {id}, name: {name}})", MapUtil.map("id", i, "name", "name"+i));
		db.execute("Create constraint on (n:Foo) assert n.name is UNIQUE");
		testCall(db, "CALL apoc.refactor.rename.nodeProperty({oldName},{newName},{nodes})",
				map("oldName", "name", "newName", "surname","nodes",mockNodes()), (r) -> {});

		assertEquals(new Long(3), resultNodesMatches(null, "surname"));
		assertEquals(new Long(7), resultNodesMatches(null, "name"));
	}

	@Test
	public void testRenameTypeProperty() throws Exception {
		for (int i = 0; i < 10; i++)
			db.execute("CREATE (f:Foo {id: {id}})-[:KNOWS {name: {name}}]->(:Fii)", MapUtil.map("id", i, "name", "name"+i));
		testCall(db, "CALL apoc.refactor.rename.typeProperty({oldName},{newName})",
				map("oldName", "name", "newName", "surname"), (r) -> {});

		assertEquals(new Long(10), resultRelationshipsMatches(null, "surname"));
		assertEquals(new Long(0), resultRelationshipsMatches(null, "name"));
	}

	@Test
	public void testRenamePropertyForSomeRelationship() throws Exception {
		for (int i = 0; i < 10; i++)
			db.execute("CREATE (f:Foo {id: {id}})-[:KNOWS {name: {name}}]->(l:Fii {id: {id}})", MapUtil.map("id", i,"name","name"+i));
		Result result = db.execute("MATCH (:Foo)-[r:KNOWS]->(:Fii) RETURN r LIMIT 2");
		testCall(db, "CALL apoc.refactor.rename.typeProperty({oldName},{newName},{rels})",
				map("oldName", "name", "newName", "surname","rels",mockRelationships(result)), (r) -> {});

		assertEquals(new Long(2), resultRelationshipsMatches(null, "surname"));
		assertEquals(new Long(8), resultRelationshipsMatches(null, "name"));
	}

	private Long resultRelationshipsMatches(String type, String prop){
		String query = type != null ? "MATCH ()-[r:"+type+"]->() RETURN count(r) as countResult" : "match ()-[r]->() where exists (r."+prop+") return count(r) as countResult";
		Result result = db.execute(query);
		Long countLoves = (Long) result.next().get("countResult");
		result.close();
		return  countLoves;
	}

	private Long resultNodesMatches(String label, String prop){
		String query = label != null ? "MATCH (b:"+label+") RETURN count(b) as countResult" : "match (n) where exists (n."+prop+") return count(n) as countResult";
		Result result = db.execute(query);
		Long countBar = (Long) result.next().get("countResult");
		result.close();
		return countBar;
	}

	private List<Relationship> mockRelationships(Result result){
		Relationship r1 = (Relationship) result.next().get("r");
		Relationship r2 = (Relationship) result.next().get("r");
		return Arrays.asList(r1, r2);
	}

	private List<Node> mockNodes(){
		Node node0 = new VirtualNode(0, db);
		Node node1 = new VirtualNode(1, db);
		Node node2 = new VirtualNode(2, db);
		return Arrays.asList(node0, node1,node2);
	}
}