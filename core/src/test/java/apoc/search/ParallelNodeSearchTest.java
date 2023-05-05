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
package apoc.search;

import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static org.junit.Assert.assertEquals;

public class ParallelNodeSearchTest {

	@ClassRule
	public static DbmsRule db = new ImpermanentDbmsRule();

	@BeforeClass
    public static void initDb() throws Exception {
		TestUtil.registerProcedure(db, ParallelNodeSearch.class);

		db.executeTransactionally(Util.readResourceFile("movies.cypher"));
    }

    @Test
    public void testMultiSearchNode() throws Throwable {
    	String query = "call apoc.search.node('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','CONTAINS','her') yield node as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(6L,row.get("c")));
    	query = "call apoc.search.node('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','STARTS WITH','Tom') yield node as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(4L,row.get("c")));
    	query = "call apoc.search.node('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','ENDS WITH','s') yield node as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(29L,row.get("c")));
    }

    @Test 
    public void testMultiSearchNodeAll() throws Throwable {
    	String query = "call apoc.search.nodeAll('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','CONTAINS','her') yield node as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(6L,row.get("c")));
    	query = "call apoc.search.nodeAll('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','STARTS WITH','Tom') yield node as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(4L,row.get("c")));
    	query = "call apoc.search.nodeAll('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','ENDS WITH','s') yield node as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(29L,row.get("c")));
    }
    
    @Test 
    public void testMultiSearchNodeReduced() throws Throwable {
    	String query = "call apoc.search.nodeReduced('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','CONTAINS','her') yield id as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(6L,row.get("c")));
    	query = "call apoc.search.nodeReduced('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','STARTS WITH','Tom') yield values as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(4L,row.get("c")));
    	query = "call apoc.search.nodeReduced('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','ENDS WITH','s') yield labels as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(29L,row.get("c")));
    }

    @Test 
    public void testMultiSearchNodeAllReduced() throws Throwable {
    	String query = "call apoc.search.nodeAllReduced('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','CONTAINS','her') yield labels as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(6L,row.get("c")));
    	query = "call apoc.search.nodeAllReduced('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','STARTS WITH','Tom') yield values as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(4L,row.get("c")));
    	query = "call apoc.search.nodeAllReduced('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','ENDS WITH','s') yield id as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(29L,row.get("c")));
    }
    @Test
    public void testMultiSearchNodeAllReducedMapParam() throws Throwable {
    	String query = "call apoc.search.nodeAllReduced({Person: 'name', Movie: ['title','tagline']},'CONTAINS','her') yield labels as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(6L,row.get("c")));
    	query = "call apoc.search.nodeAllReduced({Person: 'name', Movie: ['title','tagline']},'STARTS WITH','Tom') yield values as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(4L,row.get("c")));
    	query = "call apoc.search.nodeAllReduced({Person: 'name', Movie: ['title','tagline']},'ENDS WITH','s') yield id as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(29L,row.get("c")));
    }
    @Test
    public void testMultiSearchNodeNumberComparison() throws Throwable {
    	String query = "call apoc.search.nodeAllReduced({Person: 'born', Movie: ['released']},'>',2000) yield labels as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(12L,row.get("c")));
    }

    @Test
    public void testMultiSearchNodeNumberExactComparison() throws Throwable {
    	String query = "call apoc.search.nodeAllReduced({Person: 'born', Movie: ['released']},'=',2000) yield labels as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(3L,row.get("c")));
    	query = "call apoc.search.nodeAllReduced({Person: 'born', Movie: ['released']},'exact',2000) yield labels as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(3L,row.get("c")));
    }
}
