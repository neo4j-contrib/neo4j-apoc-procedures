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
package apoc.index;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Collections.emptyList;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 23.05.16
 */
public class
SchemaIndexTest {

    private static final String SCHEMA_DISTINCT_COUNT_ORDERED = "CALL apoc.schema.properties.distinctCount($label, $key)\n" +
            "YIELD label, key, value, count\n" +
            "RETURN * ORDER BY label, key, value";
    private static final String FULL_TEXT_LABEL = "FullTextOne";
    private static final String SCHEMA_LABEL = "SchemaTest";
    private static final String FULL_TEXT_TWO_LABEL = "FullTextTwo";
    
    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static List<String> personNames;
    private static List<String> personAddresses;
    private static List<Long> personAges;
    private static List<Long> personIds;
    private static final int firstPerson = 1;
    private static final int lastPerson = 200;

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, SchemaIndex.class);
        db.executeTransactionally("CREATE (city:City {name:'London'}) WITH city UNWIND range("+firstPerson+","+lastPerson+") as id CREATE (:Person {name:'name'+id, id:id, age:id % 100, address:id+'Main St.'})-[:LIVES_IN]->(city)");
        
        // dataset for fulltext / composite indexes
        db.executeTransactionally("CREATE (:FullTextOne {prop1: 'Michael', prop2: 111}),\n" +
                    "(:FullTextOne {prop1: 'AA', prop2: 1}),\n" +
                    "(:FullTextOne {prop1: 'EE', prop2: 111}),\n" +
                    "(:FullTextOne {prop1: 'Ryan', prop2: 1}),\n" +
                    "(:FullTextOne {prop1: 'UU', prop2: 'Ryan'}),\n" +
                    "(:FullTextOne {prop1: 'Ryan', prop2: 1}),\n" +
                    "(:FullTextOne {prop1: 'Ryan', prop3: 'qwerty'}),\n" +
                    "(:FullTextTwo {prop1: 'Ryan'}),\n" +
                    "(:FullTextTwo {prop1: 'omega'}),\n" +
                    "(:FullTextTwo {prop1: 'Ryan', prop3: 'abcde'}),\n" +
                    "(:SchemaTest {prop1: 'a', prop2: 'bar'}),\n" +
                    "(:SchemaTest {prop1: 'b', prop2: 'foo'}),\n" +
                    "(:SchemaTest {prop1: 'c', prop2: 'bar'})");
        //
        db.executeTransactionally("CREATE INDEX ON :Person(name)");
        db.executeTransactionally("CREATE INDEX ON :Person(age)");
        db.executeTransactionally("CREATE INDEX ON :Person(address)");
        db.executeTransactionally("CREATE CONSTRAINT ON (p:Person) ASSERT p.id IS UNIQUE");
        db.executeTransactionally("CREATE INDEX ON :Foo(bar)");
        db.executeTransactionally("CREATE (f:Foo {bar:'three'}), (f2a:Foo {bar:'four'}), (f2b:Foo {bar:'four'})");
        personIds = LongStream.range(firstPerson, lastPerson+1).boxed().collect(Collectors.toList());
        personNames = IntStream.range(firstPerson, lastPerson+1).mapToObj(Integer::toString).map(i -> "name"+i).sorted().collect(Collectors.toList());
        personAddresses = IntStream.range(firstPerson, lastPerson+1).mapToObj(Integer::toString).map(i -> i+"Main St.").sorted().collect(Collectors.toList());
        personAges = IntStream.range(firstPerson, lastPerson+1)
                .map(i -> i % 100)
                .sorted()
                .distinct()
                .mapToObj(Long::new).collect(Collectors.toList());
        try (Transaction tx=db.beginTx()) {
            tx.schema().awaitIndexesOnline(2,TimeUnit.SECONDS);
            tx.commit();
        }
    }

    @Test
    public void testDistinctPropertiesOnFirstIndex() throws Exception {
        testCall(db,"CALL apoc.schema.properties.distinct($label, $key)",
                map("label", "Person","key", "name"),
                (row) -> assertEquals(new HashSet<>(personNames), new HashSet<>((Collection<String>) row.get("value")))
        );
    }


    @Test(timeout = 5000L)
    public void testDistinctWithoutIndexWaitingShouldNotHangs() {
        db.executeTransactionally("CREATE FULLTEXT INDEX fulltextFullTextOne FOR (n:FullTextOne) ON EACH [n.prop1]");
        // executing the apoc.schema.properties.distinct without CALL db.awaitIndexes() will throw an "Index is still populating" exception
        
        db.executeTransactionally("CALL apoc.schema.properties.distinct($label, $key)",
                map("label", FULL_TEXT_LABEL,"key", "prop1"),
                Result::resultAsString,
                Duration.ofSeconds(10));

        db.executeTransactionally("DROP INDEX fulltextFullTextOne");
    }
    
    @Test(timeout = 5000L)
    public void testDistinctWithVoidIndexShouldNotHangs() {
        db.executeTransactionally("create index VoidIndex for (n:VoidIndex) on (n.myProp)");

        testCall(db, "CALL apoc.schema.properties.distinct($label, $key)",
                map("label", "VoidIndex", "key", "myProp"),
                row -> assertEquals(emptyList(), row.get("value"))
        );
        
        db.executeTransactionally("drop index VoidIndex");
    }

    @Test(timeout = 5000L)
    public void testDistinctWithCompositeIndexShouldNotHangs() {
        db.executeTransactionally("create index EmptyLabel for (n:EmptyLabel) on (n.one)");
        db.executeTransactionally("create index EmptyCompositeLabel for (n:EmptyCompositeLabel) on (n.two, n.three)");

        testCall(db, "CALL apoc.schema.properties.distinct($label, $key)",
                map("label", "EmptyLabel", "key", "one"),
                row -> assertEquals(emptyList(), row.get("value"))
        );

        testCall(db, "CALL apoc.schema.properties.distinct($label, $key)",
                map("label", "EmptyCompositeLabel", "key", "two"),
                row -> assertEquals(emptyList(), row.get("value"))
        );

        db.executeTransactionally("drop index EmptyLabel");
        db.executeTransactionally("drop index EmptyCompositeLabel");
    }

    @Test(timeout = 5000L)
    public void testDistinctWithCompositeIndexWithMixedRepeatedProps() {
        db.executeTransactionally("create index SchemaTest for (n:SchemaTest) on (n.prop1, n.prop2)");
        db.executeTransactionally("CALL db.awaitIndexes()");

        testResult(db, SCHEMA_DISTINCT_COUNT_ORDERED,
                map("label", SCHEMA_LABEL, "key", "prop2"),
                res -> {
                    assertDistinctCountProperties(SCHEMA_LABEL, "prop2", List.of("bar"), 2L, res);
                    assertDistinctCountProperties(SCHEMA_LABEL, "prop2", List.of("foo"), 1L, res);
                    assertFalse(res.hasNext());
                });

        testCall(db, "CALL apoc.schema.properties.distinct($label, $key)",
                map("label", SCHEMA_LABEL, "key", "prop2"),
                row -> assertEquals(Set.of("bar", "foo"), Set.copyOf((List)row.get("value")))
        );

        testResult(db, SCHEMA_DISTINCT_COUNT_ORDERED,
                map("label","","key",""),
                (result) -> {
                    extractedFoo(result);
                    extractedPerson(result);
                    extractedSchemaTest(result);
                    assertFalse(result.hasNext());
                });

        testResult(db,SCHEMA_DISTINCT_COUNT_ORDERED,
                map("label", SCHEMA_LABEL, "key",""),
                (result) -> {
                    extractedSchemaTest(result);
                    assertFalse(result.hasNext());
                });

        db.executeTransactionally("drop index SchemaTest");
    }

    @Test(timeout = 5000L)
    public void testDistinctWithFullTextIndexShouldNotHangs() {
        db.executeTransactionally("CREATE FULLTEXT INDEX FullTextOneProp1 FOR (n:FullTextOne) ON EACH [n.prop1]");
        db.executeTransactionally("CALL db.awaitIndexes");

        testCall(db, "CALL apoc.schema.properties.distinct($label, $key)",
                map("label", FULL_TEXT_LABEL, "key", "prop1"), 
                row -> assertEquals(Set.of("AA", "EE", "UU", "Ryan", "Michael"), Set.copyOf((List)row.get("value"))) 
        );
        
        testResult(db, SCHEMA_DISTINCT_COUNT_ORDERED,
                map("label", FULL_TEXT_LABEL, "key", "prop1"),
                res -> {
                    extractedFullTextFullTextOneProp1(res);
                    assertFalse(res.hasNext());
                });

        db.executeTransactionally("DROP INDEX FullTextOneProp1");
    }
    
    @Test(timeout = 5000L)
    public void testWithDifferentIndexesAndSameLabelProp() {
        db.executeTransactionally("CREATE FULLTEXT INDEX FullTextOneProp1 FOR (n:FullTextOne) ON EACH [n.prop1]");
        db.executeTransactionally("CREATE RANGE INDEX RangeProp1 FOR (n:FullTextOne) ON (n.prop1)");
        db.executeTransactionally("CALL db.awaitIndexes");

        testCall(db, "CALL apoc.schema.properties.distinct($label, $key)",
                map("label", FULL_TEXT_LABEL, "key", "prop1"),
                row -> assertEquals(Set.of("AA", "EE", "UU", "Ryan", "Michael"), Set.copyOf((List)row.get("value")))
        );

        // in this case the procedure returns distinct rows though we have 2 different analogues indexes
        testResult(db, SCHEMA_DISTINCT_COUNT_ORDERED,
                map("label", FULL_TEXT_LABEL, "key", "prop1"),
                res -> {
                    extractedFullTextFullTextOneProp1(res);
                    assertFalse(res.hasNext());
                });

        db.executeTransactionally("DROP INDEX FullTextOneProp1");
        db.executeTransactionally("DROP INDEX RangeProp1");
    }

    @Test(timeout = 5000L)
    public void testDistinctWithMultiLabelFullTextIndexShouldNotHangs() {
        db.executeTransactionally("CREATE FULLTEXT INDEX fulltextComposite FOR (n:FullTextOne|FullTextTwo) ON EACH [n.prop1,n.prop3]");
        db.executeTransactionally("CALL db.awaitIndexes");
  
  
        testResult(db, SCHEMA_DISTINCT_COUNT_ORDERED,
                map("label", FULL_TEXT_LABEL, "key", "prop1"),
                res -> {
                    extractedFullTextFullTextOneProp1(res);
                    assertFalse(res.hasNext());
                });

        testResult(db, SCHEMA_DISTINCT_COUNT_ORDERED,
                map("label","","key",""),
                (result) -> {
                    extractedFoo(result);
                    extractedFullTextFullTextOneProp1(result);
                    extractedFullTextFullTextOneProp3(result);
                    assertDistinctCountProperties(FULL_TEXT_TWO_LABEL, "prop1", List.of("Ryan"), 2L, result);
                    assertDistinctCountProperties(FULL_TEXT_TWO_LABEL, "prop1", List.of("omega"), 1L, result);
                    assertDistinctCountProperties(FULL_TEXT_TWO_LABEL, "prop3", List.of("abcde"), 1L, result);
                    extractedPerson(result);
                    assertFalse(result.hasNext());
                });

        testResult(db,SCHEMA_DISTINCT_COUNT_ORDERED,
                map("label", FULL_TEXT_LABEL, "key",""),
                (result) -> {
                    extractedFullTextFullTextOneProp1(result);
                    extractedFullTextFullTextOneProp3(result);
                    assertFalse(result.hasNext());
                });
        
        db.executeTransactionally("DROP INDEX fulltextComposite");
    }

    @Test(timeout = 5000L)
    public void testDistinctWithNoPreviousNodesShouldNotHangs() {
        db.executeTransactionally("CREATE INDEX LabelNotExistent FOR (n:LabelNotExistent) ON n.prop");
        
        testCall(db, "CREATE (:LabelNotExistent {prop:2}) " +
                        "WITH * " +
                        "CALL apoc.schema.properties.distinct(\"LabelNotExistent\", \"prop\") " +
                        "YIELD value RETURN *", 
                r -> assertEquals(emptyList(), r.get("value"))
        );

        db.executeTransactionally("DROP INDEX LabelNotExistent");
    }

    @Test
    public void testDistinctPropertiesOnSecondIndex() throws Exception {
        testCall(db,"CALL apoc.schema.properties.distinct($label, $key)",
                map("label", "Person","key", "address"),
                (row) -> assertEquals(new HashSet<>(personAddresses), new HashSet<>((Collection<String>) row.get("value")))
        );
    }

    @Test
    public void testDistinctCountPropertiesOnFirstIndex() throws Exception {
        String label = "Person";
        String key = "name";
        testResult(db,SCHEMA_DISTINCT_COUNT_ORDERED,
                map("label",label,"key",key),
                (result) -> {
                    assertDistinctCountProperties("Person", "name", personNames, 1L, result);
                    assertFalse(result.hasNext());
        });
    }

    @Test
    public void testDistinctCountPropertiesOnSecondIndex() throws Exception {
        String label = "Person";
        String key = "address";
        testResult(db,SCHEMA_DISTINCT_COUNT_ORDERED,
                map("label",label,"key",key),
                (result) -> {
                    assertDistinctCountProperties("Person", "address", personAddresses, 1L, result);
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testDistinctCountPropertiesOnEmptyLabel() throws Exception {
        String key = "name";
        testResult(db,SCHEMA_DISTINCT_COUNT_ORDERED,
                map("label", "","key",key),
                (result) -> {
                    assertDistinctCountProperties("Person", "name", personNames, 1L, result);
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testDistinctCountPropertiesOnEmptyKey() throws Exception {
        String label = "Person";
        testResult(db,SCHEMA_DISTINCT_COUNT_ORDERED,
                map("label",label,"key",""),
                (result) -> {
                    assertDistinctCountProperties("Person", "address", personAddresses, 1L, result);
                    assertDistinctCountProperties("Person", "age", personAges, 2L, result);
                    assertDistinctCountProperties("Person", "id", personIds, 1L, result);
                    assertDistinctCountProperties("Person", "name", personNames, 1L, result);
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testDistinctCountPropertiesOnEmptyLabelAndEmptyKey() {
        testResult(db, SCHEMA_DISTINCT_COUNT_ORDERED,
                map("label","","key",""),
                (result) -> {
                    assertTrue(result.hasNext());
                    assertEquals(map("label","Foo","key","bar","value","four","count",2L),result.next());
                    assertEquals(map("label","Foo","key","bar","value","three","count",1L),result.next());
                    assertDistinctCountProperties("Person", "address", personAddresses, 1L, result);
                    assertDistinctCountProperties("Person", "age", personAges, 2L, result);
                    assertDistinctCountProperties("Person", "id", personIds, 1L, result);
                    assertDistinctCountProperties("Person", "name", personNames, 1L, result);
                    assertFalse(result.hasNext());
                });
    }

    private <T> void assertDistinctCountProperties(String label, String key, Collection<T> values, Long counts, Result result) {

        values.forEach(value -> {
            assertTrue(result.hasNext());
            Map<String, Object> map = result.next();
            assertEquals(label, map.get("label"));
            assertEquals(key, map.get("key"));
            assertEquals(value, map.get("value"));
            assertEquals(counts, map.get("count"));
        });
    }

    private void extractedFullTextFullTextOneProp1(Result res) {
        assertDistinctCountProperties(FULL_TEXT_LABEL, "prop1", List.of("AA", "EE", "Michael"), 1L, res);
        assertDistinctCountProperties(FULL_TEXT_LABEL, "prop1", List.of("Ryan"), 3L, res);
        assertDistinctCountProperties(FULL_TEXT_LABEL, "prop1", List.of("UU"), 1L, res);
    }

    private void extractedFullTextFullTextOneProp3(Result res) {
        assertDistinctCountProperties(FULL_TEXT_LABEL, "prop3", List.of("qwerty"), 1L, res);
    }

    private void extractedSchemaTest(Result result) {
        assertDistinctCountProperties(SCHEMA_LABEL, "prop1", List.of("a", "b", "c"), 1L, result);
        assertDistinctCountProperties(SCHEMA_LABEL, "prop2", List.of("bar"), 2L, result);
        assertDistinctCountProperties(SCHEMA_LABEL, "prop2", List.of("foo"), 1L, result);
    }

    private void extractedFoo(Result result) {
        assertDistinctCountProperties("Foo", "bar", List.of("four"), 2L, result);
        assertDistinctCountProperties("Foo", "bar", List.of("three"), 1L, result);
    }

    private void extractedPerson(Result result) {
        assertDistinctCountProperties("Person", "address", personAddresses, 1L, result);
        assertDistinctCountProperties("Person", "age", personAges, 2L, result);
        assertDistinctCountProperties("Person", "id", personIds, 1L, result);
        assertDistinctCountProperties("Person", "name", personNames, 1L, result);
    }
}
