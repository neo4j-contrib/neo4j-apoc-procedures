package apoc.index;

import apoc.util.TestUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static apoc.index.FreeTextSearch.KEY;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.MapUtil.map;


public class FreeTextSearchTest {
    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, FreeTextSearch.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
        db = null;
    }

    @Test
    public void shouldCreateIndexWithSingleLabel() throws Exception {
        // given
        execute("CREATE (:Person{name:'George Goldman', nick:'GeeGee'}), (:Person{name:'Cyrus Jones', age:103})");

        // when
        execute("CALL apoc.index.addAllNodes('people', {Person:['name','nick']})");

        // then
        assertSingleNode("people", termQuery("GeeGee"), hasProperty("name", "George Goldman"));
        assertSingleNode("people", termQuery("George"), hasProperty("name", "George Goldman"));
        assertSingleNode("people", termQuery("Goldman"), hasProperty("name", "George Goldman"));
        assertSingleNode("people", termQuery("Cyrus"), hasProperty("name", "Cyrus Jones"));
        assertSingleNode("people", termQuery("Jones"), hasProperty("name", "Cyrus Jones"));
    }

    @Test
    public void shouldRefuseToCreateIndexWithNoStructure() throws Exception {
        // when
        try {
            execute("CALL apoc.index.addAllNodes('empty', {})");

            fail("expected exception");
        }
        // then
        catch (QueryExecutionException e) {
            Throwable cause = e.getCause();
            for (Throwable next = cause.getCause(); next != null; ) {
                cause = next;
                next = cause.getCause();
            }
            assertThat(cause, instanceOf(IllegalArgumentException.class));
            assertEquals("No structure given.", cause.getMessage());
        }
    }

    @Test
    public void shouldCreateFreeTextIndexWithMultipleLabels() throws Exception {
        // given
        execute("CREATE (:Person{name:'Long John Silver', nick:'Cook'}), (:City{name:'London'})");

        // when
        execute("CALL apoc.index.addAllNodes('stuff', {Person:['name','nick'], City:['name']})");

        // then
        assertSingleNode("stuff", termQuery("Long"), hasProperty("name", "Long John Silver"), hasLabel("Person"));
        assertSingleNode("stuff", termQuery("John"), hasProperty("name", "Long John Silver"), hasLabel("Person"));
        assertSingleNode("stuff", termQuery("Silver"), hasProperty("name", "Long John Silver"), hasLabel("Person"));
        assertSingleNode("stuff", termQuery("London"), hasProperty("name", "London"), hasLabel("City"));
    }

    @Test
    public void shouldHandleRepeatedCalls() throws Exception {
        // given
        execute("CREATE (:Person{name:'George Goldman', nick:'GeeGee'}), (:Person{name:'Cyrus Jones', age:103})");
        execute("CALL apoc.index.addAllNodes('people', {Person:['name','nick']})");
        execute("CALL apoc.index.addAllNodes('people', {Person:['name','nick']})");

        // then
        assertSingle(search("people", "GeeGee"), hasProperty("name", "George Goldman"));
        assertSingle(search("people", "Jones"), hasProperty("name", "Cyrus Jones"));
    }
    @Test
    public void shouldQueryFreeTextIndex() throws Exception {
        // given
        execute("CREATE (:Person{name:'George Goldman', nick:'GeeGee'}), (:Person{name:'Cyrus Jones', age:103})");
        execute("CALL apoc.index.addAllNodes('people', {Person:['name','nick']})");

        // then
        assertSingle(search("people", "GeeGee"), hasProperty("name", "George Goldman"));
        assertSingle(search("people", "George"), hasProperty("name", "George Goldman"));
        assertSingle(search("people", "Goldman"), hasProperty("name", "George Goldman"));
        assertSingle(search("people", "Cyrus"), hasProperty("name", "Cyrus Jones"));
        assertSingle(search("people", "Jones"), hasProperty("name", "Cyrus Jones"));
    }

    @Test
    public void shouldEnableSearchingBySpecificField() throws Exception {
        // given
        execute("CREATE (:Person{name:'Johnny'}), (:Product{name:'Johnny'})");
        execute("CALL apoc.index.addAllNodes('stuff', {Person:['name'],Product:['name']})");

        // then
        assertSingle(search("stuff", "Person.name:Johnny"), hasLabel("Person"), not(hasLabel("Product")));
        assertSingle(search("stuff", "Product.name:Johnny"), hasLabel("Product"), not(hasLabel("Person")));
    }

    @Test
    public void shouldIndexNodesWithMultipleLabels() throws Exception {
        // given
        execute("CREATE (:Foo:Bar:Baz{name:'thing'})");
        execute("CALL apoc.index.addAllNodes('stuff', {Foo:['name'], Baz:['name'], Axe:['name']})");

        // then
        assertSingle(search("stuff", "Foo.name:thing"));
        assertSingle(search("stuff", "Baz.name:thing"));
        assertNone(search("stuff", "Bar.name:thing")); // not indexed
        assertNone(search("stuff", "Axe.name:thing")); // not available on the node
        assertSingle(search("stuff", "thing"),
                hasLabel("Foo"), hasLabel("Bar"), hasLabel("Baz"), hasProperty("name", "thing"));
    }

    @Test
    public void shouldPopulateIndexInBatches() {
        // given
        // create 90k nodes - this force 2 batches during indexing
        execute("UNWIND range(1,90000) as x CREATE (:Person{name:'person'+x})");
        execute("CALL apoc.index.addAllNodes('people', {Person:['name']})");

        // then
        assertSingle(search("people", "person89999"), hasProperty("name", "person89999"));
    }

    @Test
    public void shouldReportScoreFromIndex() throws Exception {
        // given
        db.execute("UNWIND {things} AS thing CREATE (:Thing{name:thing})", singletonMap("things",
                asList("food", "feed", "foot", "fork", "foo", "bar", "ford"))).close();
        execute("CALL apoc.index.addAllNodes('things',{Thing:['name']})");

        // when
        ResourceIterator<String> things = db.execute(
                "CALL apoc.index.search('things', 'food~')\n" +
                        "YIELD node AS thing, weight AS score\n" +
                        "RETURN thing.name").columnAs("thing.name");

        // then
        assertEquals(asList("food", "foot", "ford", "foo", "feed", "fork"), Iterators.asList(things));
    }

    @Test
    public void shouldSearchInNumericRange() throws Exception {
        // given
        execute("UNWIND range(1, 10000) AS num CREATE (:Number{name:'The ' + num + 'th',number:num})");
        execute("CALL apoc.index.addAllNodes('numbers', {Number:['name','number']})");

        // when
        ResourceIterator<Object> names = db.execute(
                "CALL apoc.index.search('numbers', 'Number.number:{100 TO 105]') YIELD node\n" +
                        "RETURN node.name").columnAs("node.name");

        // then
        assertEquals(asList("The 101th", "The 102th", "The 103th", "The 104th", "The 105th"), Iterators.asList(names));
    }

    @Test
    public void shouldLimitNumberOfResults() throws Exception {
        // given
        execute("UNWIND range(1, 10000) AS num CREATE (:Number{name:'The ' + num + 'th',number:num})");
        execute("CALL apoc.index.addAllNodes('numbers', {Number:['name','number']})");

        // when
        Result result = db.execute("CALL apoc.index.search('numbers', 'The')");

        // then
        assertEquals(100, Iterators.count(result));
    }

    @Test
    public void shouldFindWithWildcards() throws Exception {
        // given
        String john = "John Doe";
        String jim = "Jim Done";
        String fred = "Fred Finished";
        Map params = singletonMap("names", Iterators.array(john, jim, fred));
        execute("UNWIND {names} as name CREATE (:Hacker{name:name})", params);
        execute("CALL apoc.index.addAllNodes('hackerz', {Hacker:['name']})");

        // expect
        TestUtil.testResult(db, "CALL apoc.index.search('hackerz', 'D*') yield node, weight " +
                "RETURN node.name as name", result -> {
            List<Object> names = Iterators.asList(result.columnAs("name"));
            assertTrue(names.contains(jim));
            assertTrue(names.contains(john));
            assertFalse(names.contains(fred));
        });

        // expect
        TestUtil.testResult(db, "CALL apoc.index.search('hackerz', '*shed') yield node, weight " +
                "RETURN node.name as name", result -> {
            List<Object> names = Iterators.asList(result.columnAs("name"));
            assertFalse(names.contains(jim));
            assertFalse(names.contains(john));
            assertTrue(names.contains(fred));
        });

    }

    private ResourceIterator<Node> search(String index, String value) {
        return db.execute("CALL apoc.index.search({index}, {value}) YIELD node RETURN node",
                map("index", index, "value", value)).columnAs("node");
    }

    @SafeVarargs
    private final void assertSingleNode(String index, Object query, Matcher<? super Node>... matchers) {
        try (Transaction tx = db.beginTx()) {
            assertSingle(nodeIndex(index).query(query), matchers);
            tx.success();
        }
    }

    private static void assertNone(Iterator<?> iter) {
        try {
            assertFalse("should not contain any values", iter.hasNext());
        } finally {
            if (iter instanceof ResourceIterator<?>) {
                ((ResourceIterator<?>) iter).close();
            }
        }
    }

    @SafeVarargs
    private static <T extends PropertyContainer> void assertSingle(Iterator<T> iter, Matcher<? super T>... matchers) {
        try {
            assertTrue("should contain at least one value", iter.hasNext());
            assertThat(iter.next(), allOf(matchers));
            assertFalse("should contain at most one value", iter.hasNext());
        } finally {
            if (iter instanceof ResourceIterator<?>) {
                ((ResourceIterator<?>) iter).close();
            }
        }
    }

    private static Query termQuery(String value) {
        return new TermQuery(new Term(KEY, value.toLowerCase()));
    }

    private static Matcher<Node> hasLabel(String label) {
        return new TypeSafeDiagnosingMatcher<Node>() {
            @Override
            protected boolean matchesSafely(Node item, Description mismatchDescription) {
                if (item.hasLabel(label(label))) {
                    return true;
                }
                mismatchDescription.appendText("missing label ").appendValue(label);
                return false;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("Node with label ").appendValue(label);
            }
        };
    }

    private static Matcher<? super PropertyContainer> hasProperty(String key, Object value) {
        return new TypeSafeDiagnosingMatcher<PropertyContainer>() {
            @Override
            protected boolean matchesSafely(PropertyContainer item, Description mismatchDescription) {
                Object property = item.getProperty(key, null);
                if (property == null) {
                    mismatchDescription.appendText("property ").appendValue(key).appendText(" not present");
                    return false;
                }
                if (value instanceof Matcher<?>) {
                    Matcher<?> matcher = (Matcher<?>) value;
                    if (!matcher.matches(property)) {
                        matcher.describeMismatch(property, mismatchDescription);
                        return false;
                    }
                    return true;
                }
                if (!property.equals(value)) {
                    mismatchDescription.appendText("property ").appendValue(key).appendText("has value").appendValue(property);
                    return false;
                }
                return true;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("entity with property ").appendValue(key).appendText("=").appendValue(value);
            }
        };
    }

    private Index<Node> nodeIndex(String name) {
        assertTrue(db.index().existsForNodes(name));
        return db.index().forNodes(name);
    }

    private void execute(String query) {
        execute(query, Collections.EMPTY_MAP);
    }

    private void execute(String query, Map<String, Object> params) {
        db.execute(query, params).close();
    }
}
