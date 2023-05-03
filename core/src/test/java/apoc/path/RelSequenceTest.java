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
package apoc.path;

import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class RelSequenceTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, PathExplorer.class);
        String movies = Util.readResourceFile("movies.cypher");
        String additionalLink = "match (p:Person{name:'Nora Ephron'}), (m:Movie{title:'When Harry Met Sally'}) create (p)-[:ACTED_IN]->(m)";
        try (Transaction tx = db.beginTx()) {
            tx.execute(movies);
            tx.execute(additionalLink);
            tx.commit();
        }
    }

    @Test
    public void testBasicRelSequence() throws Throwable {
        String query = "MATCH (t:Person {name: 'Tom Hanks'}) CALL apoc.path.expandConfig(t,{relationshipFilter:'ACTED_IN>,<DIRECTED', labelFilter:'>Person,Movie'}) yield path with distinct last(nodes(path)) as node return collect(node.name) as names";
        TestUtil.testCall(db, query, (row) -> {
            List<String> expectedNames = new ArrayList<>(Arrays.asList("Robert Zemeckis", "Mike Nichols", "Ron Howard", "Frank Darabont", "Tom Tykwer", "Andy Wachowski", "Lana Wachowski", "Tom Hanks", "John Patrick Stanley", "Nora Ephron", "Penny Marshall", "Rob Reiner"));
            List<String> names = (List<String>) row.get("names");
            assertEquals(12l, names.size());
            assertTrue(names.containsAll(expectedNames));
        });
    }

    @Test
    public void testRelSequenceWithMinLevel() throws Throwable {
        String query = "MATCH (t:Person {name: 'Tom Hanks'}) CALL apoc.path.expandConfig(t,{relationshipFilter:'ACTED_IN>,<DIRECTED', labelFilter:'>Person,Movie', minLevel:3}) yield path with distinct last(nodes(path)) as node return collect(node.name) as names";
        TestUtil.testCall(db, query, (row) -> {
            List<String> expectedNames = new ArrayList<>(Arrays.asList("Robert Zemeckis", "Mike Nichols", "Ron Howard", "Frank Darabont", "Tom Tykwer", "Andy Wachowski", "Lana Wachowski", "John Patrick Stanley", "Nora Ephron", "Penny Marshall", "Rob Reiner"));
            List<String> names = (List<String>) row.get("names");
            assertEquals(11l, names.size());
            assertTrue(names.containsAll(expectedNames));
        });
    }

    @Test
    public void testRelSequenceWithMaxLevel() throws Throwable {
        String query = "MATCH (t:Person {name: 'Tom Hanks'}) CALL apoc.path.expandConfig(t,{relationshipFilter:'ACTED_IN>,<DIRECTED', labelFilter:'>Person,Movie', maxLevel:2}) yield path with distinct last(nodes(path)) as node return collect(node.name) as names";
        TestUtil.testCall(db, query, (row) -> {
            List<String> expectedNames = new ArrayList<>(Arrays.asList("Robert Zemeckis", "Mike Nichols", "Ron Howard", "Frank Darabont", "Tom Tykwer", "Andy Wachowski", "Lana Wachowski", "John Patrick Stanley", "Nora Ephron", "Penny Marshall", "Tom Hanks"));
            List<String> names = (List<String>) row.get("names");
            assertEquals(11l, names.size());
            assertTrue(names.containsAll(expectedNames));
        });
    }

    @Test
    public void testRelSequenceWhenNotBeginningAtStart() throws Throwable {
        String query = "MATCH (t:Person {name: 'Tom Hanks'}) CALL apoc.path.expandConfig(t,{relationshipFilter:'ACTED_IN>,<DIRECTED,ACTED_IN>', labelFilter:'Movie,>Person', beginSequenceAtStart:false}) yield path with distinct last(nodes(path)) as node return collect(node.name) as names";
        TestUtil.testCall(db, query, (row) -> {
            List<String> expectedNames = new ArrayList<>(Arrays.asList("Robert Zemeckis", "Mike Nichols", "Ron Howard", "Frank Darabont", "Tom Tykwer", "Andy Wachowski", "Lana Wachowski", "Tom Hanks", "John Patrick Stanley", "Nora Ephron", "Penny Marshall", "Rob Reiner"));
            List<String> names = (List<String>) row.get("names");
            assertEquals(12l, names.size());
            assertTrue(names.containsAll(expectedNames));
        });
    }

    @Test
    public void testRelationshipFilterWorksWithoutTypeWithRelSequence() {
        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}) " +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'>,<', labelFilter:'/Person'}) yield node " +
                        "return collect(node.name) as names",
                result -> {
                    List<String> expectedNames = new ArrayList<>(Arrays.asList("Nancy Meyers", "Jack Nicholson", "Diane Keaton", "Dina Meyer", "Ice-T", "Takeshi Kitano", "Robert Longo", "Jessica Thompson", "Angela Scope", "James Thompson", "Brooke Langton", "Gene Hackman", "Orlando Jones", "Howard Deutch", "Al Pacino", "Taylor Hackford", "Charlize Theron", "Lana Wachowski", "Joel Silver", "Hugo Weaving", "Andy Wachowski", "Carrie-Anne Moss", "Laurence Fishburne", "Emil Eifrem"));
                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    List<String> names = (List<String>) maps.get(0).get("names");
                    assertEquals(24, names.size());
                    assertTrue(names.containsAll(expectedNames));
                });
    }
}
