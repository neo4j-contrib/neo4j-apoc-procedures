package apoc.agg;

import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Arrays;
import java.util.List;

import static apoc.util.TestUtil.testCall;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;


public class MaxAndMinItemsAggregationTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, MaxAndMinItems.class);

        String movies = Util.readResourceFile("movies.cypher");
        String bigbrother = "MATCH (per:Person) MERGE (bb:BigBrother {name : 'Big Brother' })  MERGE (bb)-[:FOLLOWS]->(per)";
        try (Transaction tx = db.beginTx()) {
            tx.execute(movies);
            tx.execute(bigbrother);
            tx.commit();
        }
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testBasicMax() throws Exception {
        testCall(db, "UNWIND RANGE(0,10) as value " +
                        "WITH apoc.agg.maxItems(value, value) as maxResult " +
                        "RETURN maxResult.value as value, maxResult.items as items",
                (row) -> {
                    assertEquals(10L, row.get("value"));
                    assertThat((List<Object>) row.get("items"), iterableWithSize(1));
                    assertThat((List<Object>) row.get("items"), contains(10L));
                });


        testCall(db, "UNWIND RANGE(0,10) as value " +
                        "WITH apoc.agg.maxItems(value, value) as maxResult " +
                        "ORDER BY maxResult.value DESC " +
                        "RETURN maxResult.value as value, maxResult.items as items",
                (row) -> {
                    assertEquals(10L, row.get("value"));
                    assertThat((List<Object>) row.get("items"), iterableWithSize(1));
                    assertThat((List<Object>) row.get("items"), contains(10L));
                });
    }

    @Test
    public void testBasicMin() throws Exception {
        testCall(db, "UNWIND RANGE(0,10) as value " +
                        "WITH apoc.agg.minItems(value, value) as minResult " +
                        "RETURN minResult.value as value, minResult.items as items",
                (row) -> {
                    assertEquals(0L, row.get("value"));
                    assertThat((List<Object>) row.get("items"), iterableWithSize(1));
                    assertThat((List<Object>) row.get("items"), contains(0L));
                });


        testCall(db, "UNWIND RANGE(0,10) as value " +
                        "WITH apoc.agg.minItems(value, value) as minResult " +
                        "ORDER BY minResult.value DESC " +
                        "RETURN minResult.value as value, minResult.items as items",
                (row) -> {
                    assertEquals(0L, row.get("value"));
                    assertThat((List<Object>) row.get("items"), iterableWithSize(1));
                    assertThat((List<Object>) row.get("items"), contains(0L));
                });
    }

    @Test
    public void testMaxWithGrouping() throws Exception {
        /** comparing to:
          MATCH (p:Person)
          WHERE p.born <= 1974
          WITH p.born as born, collect(p.name) as persons
          ORDER BY born DESC
          LIMIT 1
          RETURN born, persons

          * returns {born:1974, persons:["Jerry O'Connell", "Christian Bale"]}
         */

        testCall(db, "MATCH (p:Person) " +
                        "WHERE p.born <= 1974 " +
                        "WITH apoc.agg.maxItems(p, p.born) as maxResult " +
                        "RETURN maxResult.value as born, [person in maxResult.items | person.name] as persons",
                (row) -> {
                    assertEquals(1974L, row.get("born"));
                    assertThat((List<Object>)row.get("persons"), iterableWithSize(2));
                    assertThat((List<Object>)row.get("persons"), containsInAnyOrder("Jerry O'Connell", "Christian Bale"));
                });
    }

    @Test
    public void testMinWithGrouping() throws Exception {
        /** comparing to:
         MATCH (p:Person)
         WHERE p.born >= 1930
         WITH p.born as born, collect(p.name) as persons
         ORDER BY born ASC
         LIMIT 1
         RETURN born, persons

         * returns {born:1930, persons:["Gene Hackman", "Richard Harris", "Clint Eastwood"]}
         * with limited grouping will return a limited subset of the results
         */

        testCall(db, "MATCH (p:Person) " +
                        "WHERE p.born >= 1930 " +
                        "WITH apoc.agg.minItems(p, p.born) as minResult " +
                        "RETURN minResult.value as born, [person in minResult.items | person.name] as persons",
                (row) -> {
                    assertEquals(1930L, row.get("born"));
                    assertThat((List<Object>)row.get("persons"), iterableWithSize(3));
                    assertThat((List<Object>)row.get("persons"), containsInAnyOrder("Gene Hackman", "Richard Harris", "Clint Eastwood"));
                });
    }

    @Test
    public void testMaxWithLimitedGrouping() throws Exception {
        /** comparing to:
         MATCH (p:Person)
         WHERE p.born <= 1974
         WITH p.born as born, collect(p.name) as persons
         ORDER BY born DESC
         LIMIT 1
         RETURN born, persons

         * returns {born:1974, persons:["Jerry O'Connell", "Christian Bale"]}
         * with limited grouping will return a limited subset of the results
         */

        testCall(db, "MATCH (p:Person) " +
                        "WHERE p.born <= 1974 " +
                        "WITH apoc.agg.maxItems(p, p.born, 1) as maxResult " +
                        "RETURN maxResult.value as born, [person in maxResult.items | person.name] as persons",
                (row) -> {
                    assertEquals(1974L, row.get("born"));
                    List<String> persons = (List<String>)row.get("persons");
                    assertThat(persons.size(), equalTo(1));
                    assertTrue(Arrays.asList("Jerry O'Connell", "Christian Bale").containsAll(persons));
                });
    }

    @Test
    public void testMinWithLimitedGrouping() throws Exception {
        /** comparing to:
         MATCH (p:Person)
         WHERE p.born >= 1930
         WITH p.born as born, collect(p.name) as persons
         ORDER BY born ASC
         LIMIT 1
         RETURN born, persons

         * returns {born:1930, persons:["Gene Hackman", "Richard Harris", "Clint Eastwood"]}
         * with limited grouping will return a limited subset of the results
         */

        testCall(db, "MATCH (p:Person) " +
                        "WHERE p.born >= 1930 " +
                        "WITH apoc.agg.minItems(p, p.born, 2) as minResult " +
                        "RETURN minResult.value as born, [person in minResult.items | person.name] as persons",
                (row) -> {
                    assertEquals(1930L, row.get("born"));
                    List<String> persons = (List<String>)row.get("persons");
                    assertThat(persons.size(), equalTo(2));
                    assertTrue(Arrays.asList("Gene Hackman", "Richard Harris", "Clint Eastwood").containsAll(persons));
                });
    }

    @Test
    public void testMaxWithNullValuesProducesNoResults() throws Exception {
        testCall(db,  "MATCH (p:Person) " +
                        "WITH apoc.agg.maxItems(p, p.doesNotExist) as maxResult " +
                        "RETURN maxResult.value as value, [person in maxResult.items | person.name] as persons",
                (row) -> {
                    assertEquals(null, row.get("value"));
                    assertThat((List<Object>)row.get("persons"), iterableWithSize(0));
                });
    }

    @Test
    public void testMinWithNullValuesProducesNoResults() throws Exception {
        testCall(db,  "MATCH (p:Person) " +
                        "WITH apoc.agg.minItems(p, p.doesNotExist) as minResult " +
                        "RETURN minResult.value as value, [person in minResult.items | person.name] as persons",
                (row) -> {
                    assertEquals(null, row.get("value"));
                    assertThat((List<Object>)row.get("persons"), iterableWithSize(0));
                });
    }
}

