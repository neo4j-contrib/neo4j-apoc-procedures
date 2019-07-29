package apoc.refactor.rename;


import apoc.util.MapUtil;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

/**
 * @author AgileLARUS
 *
 * @since 03-04-2017
 */
public class RenameTest {

	@Rule
	public static DbmsRule db = new ImpermanentDbmsRule();

	@Before public void setUp() throws Exception {
		TestUtil.registerProcedure(db, Rename.class);
	}

	@Test
	public void testRenameLabelForSomeNodes() throws Exception {
		List<Node> nodes = db.execute("UNWIND range(0,9) as id CREATE (f:Foo {id: id, name: 'name'+id}) RETURN f").<Node>columnAs("f").stream().collect(Collectors.toList());
		testCall(db, "CALL apoc.refactor.rename.label({oldName},{newName}, {nodes})",
				map("oldName", "Foo", "newName", "Bar", "nodes", nodes.subList(0,3)), (r) -> {});

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
		List<Node> nodes = db.execute("UNWIND range(0,9) as id CREATE (f:Foo {id: id, name: 'name'+id}) RETURN f").<Node>columnAs("f").stream().collect(Collectors.toList());
		testCall(db, "CALL apoc.refactor.rename.nodeProperty({oldName},{newName})",
				map("oldName", "name", "newName", "surname"), (r) -> {});

		assertEquals(new Long(10), resultNodesMatches(null, "surname"));
		assertEquals(new Long(0), resultNodesMatches(null, "name"));
	}

	@Test
	public void testRenamePropertyForSomeNodes() throws Exception {
		List<Node> nodes = db.execute("UNWIND range(0,9) as id CREATE (f:Foo {id: id, name: 'name'+id}) RETURN f").<Node>columnAs("f").stream().collect(Collectors.toList());
		db.execute("Create constraint on (n:Foo) assert n.name is UNIQUE");
		testCall(db, "CALL apoc.refactor.rename.nodeProperty({oldName},{newName},{nodes})",
				map("oldName", "name", "newName", "surname","nodes",nodes.subList(0,3)), (r) -> {});

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
}
