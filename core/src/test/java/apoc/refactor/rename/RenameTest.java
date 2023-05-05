/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.refactor.rename;


import apoc.coll.Coll;
import apoc.lock.Lock;
import apoc.util.TestUtil;
import apoc.util.Utils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallCount;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.lock_acquisition_timeout;
import static org.neo4j.test.assertion.Assert.assertEventually;

/**
 * @author AgileLARUS
 *
 * @since 03-04-2017
 */
public class RenameTest {

	@Rule
	public DbmsRule db = new ImpermanentDbmsRule()
			.withSetting(lock_acquisition_timeout, Duration.ofSeconds(5));

	@Before public void setUp() throws Exception {
		TestUtil.registerProcedure(db, Rename.class, Coll.class, Lock.class, Utils.class);
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
	public void testRenameLabelDoesntAllowCypherInjection() throws Exception {
		db.executeTransactionally("UNWIND range(0,9) AS id CREATE (f:Foo {id: id})");
		testCall(db, "CALL apoc.refactor.rename.label(" +
						"  'Foo', " +
						"  'Whatever` WITH n MATCH (m) DETACH DELETE m //'" +
						")",
				map(),
				(r) -> {});

		assertEquals(10L, resultNodesMatches("Whatever`` WITH n MATCH (m) DETACH DELETE m //", null));
		assertEquals(0L, resultNodesMatches("Foo", null));

		testCall(db, "CALL apoc.refactor.rename.label(" +
						"  'Whatever` WITH n MATCH (m) DETACH DELETE m //', " +
						"  'Foo'" +
						")",
				map(),
				(r) -> {});

		assertEquals(0L, resultNodesMatches("Whatever`` WITH n MATCH (m) DETACH DELETE m //", null));
		assertEquals(10L, resultNodesMatches("Foo", null));
	}

	@Test
	public void testRenameLabelDoesntAllowCypherInjectionForSomeNodes() throws Exception {
		List<Node> nodes = TestUtil.firstColumn(db, "UNWIND range(0,9) as id CREATE (f:Foo {id: id, name: 'name'+id}) RETURN f");
		testCall(db, "CALL apoc.refactor.rename.label(" +
						"  'Foo', " +
						"  'Whatever` WITH n MATCH (m) DETACH DELETE m //'," +
						"   $nodes" +
						")",
				map("nodes", nodes.subList(0,3)),
				(r) -> {});
		testCall(db, "CALL apoc.refactor.rename.label(" +
						"  'Foo', " +
						"  'Whatever\u0060 WITH n MATCH (m) DETACH DELETE m //'," +
						"   $nodes" +
						")",
				map("nodes", nodes.subList(4,6)),
				(r) -> {});

		assertEquals(5L, resultNodesMatches("Whatever`` WITH n MATCH (m) DETACH DELETE m //", null));
		assertEquals(5L, resultNodesMatches("Foo", null));

		testCall(db, "CALL apoc.refactor.rename.label(" +
						"  'Whatever` WITH n MATCH (m) DETACH DELETE m //', " +
						"  'Bar'" +
						")",
				map(),
				(r) -> {});

		assertEquals(0L, resultNodesMatches("Whatever`` WITH n MATCH (m) DETACH DELETE m //", null));
		assertEquals(5L, resultNodesMatches("Bar", null));
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
	public void testRenameTypeDoesntAllowCypherInjection() throws Exception {
		db.executeTransactionally("UNWIND range(0,9) AS id CREATE (f:Foo {id: id})-[:KNOWS {id: id}]->(l:Fii {id: id})");
		testCall(db, "CALL apoc.refactor.rename.type(" +
						"  'KNOWS', " +
						"  'Whatever` WITH n MATCH (m) DETACH DELETE m //'" +
						")",
				map(),
				(r) -> {});

		assertEquals(10L, resultRelationshipsMatches("Whatever`` WITH n MATCH (m) DETACH DELETE m //", null));
		assertEquals(0L, resultRelationshipsMatches("KNOWS", null));

		testCall(db, "CALL apoc.refactor.rename.type(" +
						"  'Whatever` WITH n MATCH (m) DETACH DELETE m //', " +
						"  'KNOWS'" +
						")",
				map(),
				(r) -> {});

		assertEquals(0L, resultRelationshipsMatches("Whatever`` WITH n MATCH (m) DETACH DELETE m //", null));
		assertEquals(10L, resultRelationshipsMatches( "KNOWS", null));
	}
	@Test
	public void testRenameTypeDoesntAllowCypherInjectionForSomeRelationships() throws Exception {
		db.executeTransactionally("UNWIND range(0,9) AS id CREATE (f:Foo {id: id})-[:KNOWS {id: id}]->(l:Fii {id: id})");

		List<Relationship> rels = TestUtil.firstColumn(db, "MATCH (:Foo)-[r:KNOWS]->(:Fii) RETURN r LIMIT 4");

		testCall(db, "CALL apoc.refactor.rename.type(" +
						"  'KNOWS', " +
						"  'Whatever` WITH n MATCH (m) DETACH DELETE m //'," +
						"  $rels" +
						")",
				map("rels", rels.subList(0,2)),
				(r) -> {});

		testCall(db, "CALL apoc.refactor.rename.type(" +
						"  'KNOWS', " +
						"  'Whatever\u0060 WITH n MATCH (m) DETACH DELETE m //'," +
						"  $rels" +
						")",
				map("rels", rels.subList(2,4)),
				(r) -> {});

		assertEquals(4L, resultRelationshipsMatches("Whatever`` WITH n MATCH (m) DETACH DELETE m //", null));
		assertEquals(6L, resultRelationshipsMatches("KNOWS", null));

		testCall(db, "CALL apoc.refactor.rename.type(" +
						"  'Whatever` WITH n MATCH (m) DETACH DELETE m //', " +
						"  'LIKES'" +
						")",
				map(),
				(r) -> {});

		assertEquals(0L, resultRelationshipsMatches("Whatever`` WITH n MATCH (m) DETACH DELETE m //", null));
		assertEquals(4L, resultRelationshipsMatches( "LIKES", null));
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
	public void testRenamePropertyDoesntAllowCypherInjection() throws Exception {
		db.executeTransactionally("UNWIND range(0,9) as id CREATE (f:Foo {id: id, name: 'name'+id})");
		testCall(db, "CALL apoc.refactor.rename.nodeProperty(" +
						"  'name', " +
						"  'Whatever` WITH n MATCH (m) DETACH DELETE m //'" +
						")",
				map(),
				(r) -> {});

		assertEquals(10L, resultNodesMatches(null, "Whatever`` WITH n MATCH (m) DETACH DELETE m //"));
		assertEquals(0L, resultNodesMatches(null, "name"));

		testCall(db, "CALL apoc.refactor.rename.nodeProperty(" +
						"  'Whatever` WITH n MATCH (m) DETACH DELETE m //', " +
						"  'name'" +
						")",
				map(),
				(r) -> {});

		assertEquals(0L, resultNodesMatches(null, "Whatever`` WITH n MATCH (m) DETACH DELETE m //"));
		assertEquals(10L, resultNodesMatches(null, "name"));
	}
	@Test
	public void testRenamePropertyDoesntAllowCypherInjectionForSomeNodes() throws Exception {
		List<Node> nodes = TestUtil.firstColumn(db, "UNWIND range(0,9) as id CREATE (f:Foo {id: id, name: 'name'+id}) RETURN f");
		testCall(db, "CALL apoc.refactor.rename.nodeProperty(" +
						"  'name', " +
						"  'Whatever` WITH n MATCH (m) DETACH DELETE m //'," +
						"	$nodes" +
						")",
				map("nodes", nodes.subList(0,3)),
				(r) -> {});
		testCall(db, "CALL apoc.refactor.rename.nodeProperty(" +
						"  'name', " +
						"  'Whatever\u0060 WITH n MATCH (m) DETACH DELETE m //'," +
						"	$nodes" +
						")",
				map("nodes", nodes.subList(4,6)),
				(r) -> {});

		assertEquals(5L, resultNodesMatches(null, "Whatever`` WITH n MATCH (m) DETACH DELETE m //"));
		assertEquals(5L, resultNodesMatches(null, "name"));

		testCall(db, "CALL apoc.refactor.rename.nodeProperty(" +
						"  'Whatever` WITH n MATCH (m) DETACH DELETE m //', " +
						"  'surname'" +
						")",
				map(),
				(r) -> {});

		assertEquals(0L, resultNodesMatches(null, "Whatever`` WITH n MATCH (m) DETACH DELETE m //"));
		assertEquals(5L, resultNodesMatches(null, "surname"));
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
	public void testRenameTypePropertyDoesntAllowCypherInjection() throws Exception {
		db.executeTransactionally("UNWIND range(0,9) as id CREATE (f:Foo {id: id})-[:KNOWS {name: 'name' +id}]->(:Fii)");
		testCall(db, "CALL apoc.refactor.rename.typeProperty(" +
						"  'name', " +
						"  'Whatever ` = null remove r.name //'" +
						")",
				map(),
				(r) -> {});

		assertEquals(10L, resultRelationshipsMatches(null, "Whatever `` = null remove r.name //"));
		assertEquals(0L, resultRelationshipsMatches(null, "name"));

		testCall(db, "CALL apoc.refactor.rename.typeProperty(" +
						"  'Whatever ` = null remove r.name //', " +
						"  'name'" +
						")",
				map(),
				(r) -> {});

		assertEquals(0L, resultRelationshipsMatches(null, "Whatever `` = null remove r.name //"));
		assertEquals(10L, resultRelationshipsMatches(null, "name"));
	}

	@Test
	public void testRenameTypePropertyDoesntAllowCypherInjectionForSomeRelationships() throws Exception {
		db.executeTransactionally("UNWIND range(0,9) as id CREATE (f:Foo {id: id})-[:KNOWS {name: 'name' +id}]->(:Fii)");
		List<Relationship> rels = TestUtil.firstColumn(db, "MATCH (:Foo)-[r:KNOWS]->(:Fii) RETURN r LIMIT 2");
		testCall(db, "CALL apoc.refactor.rename.typeProperty(" +
						"  'name', " +
						"  'Whatever ` = null remove r.name //'," +
						"	$rels" +
						")",
				map("rels",rels),
				(r) -> {});

		assertEquals(2L, resultRelationshipsMatches(null, "Whatever `` = null remove r.name //"));
		assertEquals(8L, resultRelationshipsMatches(null, "name"));

		testCall(db, "CALL apoc.refactor.rename.typeProperty(" +
						"  'Whatever ` = null remove r.name //', " +
						"  'surname'" +
						")",
				map(),
				(r) -> {});

		assertEquals(0L, resultRelationshipsMatches(null, "Whatever `` = null remove r.name //"));
		assertEquals(2L, resultRelationshipsMatches(null, "surname"));
	}

	@Test
	public void testLockException() {
		String query = "MERGE (account1:Account{ID: 1})\n" +
				"MERGE (account2:Account{ID: 2})\n" +
				"WITH account1, account2\n" +
				"MERGE (account1)-[:SIMILAR_TO {id: 1}]->(account2)\n" +
				"MERGE (account1)-[:SIMILAR_TO {id: 2}]->(account2)";
		db.executeTransactionally(query);

		final String lockQuery = "match (n:Account {ID:1})-[r:SIMILAR_TO {id: 1}]->(:Account) set r.other=123 " +
				"with r call apoc.util.sleep(20000) return r";
		new Thread(() -> db.executeTransactionally(lockQuery)).start();
		
		// check until the lock query is running
		assertEventually(() -> db.executeTransactionally("SHOW TRANSACTIONS YIELD currentQuery, status " +
								"WHERE status = 'Running' RETURN currentQuery",
						Collections.emptyMap(), 
						r -> r.<String>columnAs("currentQuery")
								.stream().anyMatch(curr -> curr.contains(lockQuery))),
				(v) -> v, 20, TimeUnit.SECONDS);

		String testQuery = "MATCH (:Account)-[r:SIMILAR_TO]->(:Account)\n" +
				"WITH COLLECT(r) as rs\n" +
				"CALL apoc.refactor.rename.type('SIMILAR_TO','SIMILAR_TO_'+rand(),rs, {batchSize:10}) YIELD batch\n" +
				"RETURN batch";
		testResult(db, testQuery, Collections.emptyMap(), (r) -> {
			final Map<String, Object> batch = r.<Map<String, Object>>columnAs("batch").next();
			final Map<String, Object> errors = (Map<String, Object>) batch.get("errors");
			assertTrue(errors.keySet().stream().anyMatch(i->i.contains("Unable to acquire lock")));
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
	
	@Test
	public void testRenameWithSameValues() {
		db.executeTransactionally("CREATE (n:ToRename {a: 1})-[:REL_TO_RENAME {a: 1}]->(:Other)");
		testCall(db, "CALL apoc.refactor.rename.label('ToRename', 'ToRename')", r -> assertEquals(1L, r.get("total")));
		testCallCount(db, "MATCH (n:ToRename {a: 1}) RETURN n", 1);

		testCall(db, "CALL apoc.refactor.rename.type('REL_TO_RENAME', 'REL_TO_RENAME')", r -> assertEquals(1L, r.get("total")));
		testCallCount(db, "MATCH (:ToRename)-[r:REL_TO_RENAME {a: 1}]->(:Other) RETURN r", 1);

		testCall(db, "CALL apoc.refactor.rename.nodeProperty('a', 'a')", r -> assertEquals(1L, r.get("total")));
		testCallCount(db, "MATCH (n:ToRename {a: 1}) RETURN n", 1);

		testCall(db, "CALL apoc.refactor.rename.typeProperty('a', 'a')", r -> assertEquals(1L, r.get("total")));
		testCallCount(db, "MATCH (:ToRename)-[r:REL_TO_RENAME {a: 1}]->(:Other) RETURN r", 1);
	}

	private long resultRelationshipsMatches(String type, String prop){
		String query = type != null ? "MATCH ()-[r:`"+type+"`]->() RETURN count(r) as countResult" : "match ()-[r]->() where exists (r.`"+prop+"`) return count(r) as countResult";
		return TestUtil.singleResultFirstColumn(db, query);
	}

	private long resultNodesMatches(String label, String prop) {
		String query = label != null ? "MATCH (b:`"+label+"`) RETURN count(b) as countResult" : "match (n) where exists (n.`"+prop+"`) return count(n) as countResult";
		return TestUtil.singleResultFirstColumn(db, query);
	}

}
