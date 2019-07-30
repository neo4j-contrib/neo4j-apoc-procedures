package apoc.index;

import apoc.index.analyzer.DynamicChainAnalyzer;
import apoc.util.TestUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.*;

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
        TestUtil.registerProcedure(db, FulltextIndex.class);
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
        execute("UNWIND range(1,55000) as x CREATE (:Person{name:'person'+x})");
        execute("CALL apoc.index.addAllNodes('people', {Person:['name']})");

        // then
        assertSingle(search("people", "person54999"), hasProperty("name", "person54999"));
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
        assertThat(Iterators.asList(names), Matchers.containsInAnyOrder("The 101th", "The 102th", "The 103th", "The 104th", "The 105th"));
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

        // when
        result = db.execute("CALL apoc.index.search('numbers', 'The', 10)");

        // then
        assertEquals(10, Iterators.count(result));

        // when
        result = db.execute("CALL apoc.index.search('numbers', 'The', -1)");

        // then
        assertEquals(10000, Iterators.count(result));
    }


    @Test
    @Ignore("until this is sorted out: Write operations are not allowed for AUTH_DISABLED with FULL restricted to READ - LegacyIndexProxy.internalRemove ")
    public void shouldHandleDeletion() throws Exception {
        // given
        String john = "John Doe";
        String jim = "Jim Done";
        Map params = singletonMap("names", Iterators.array(john, jim));
        execute("UNWIND {names} as name CREATE (:Hacker{name:name})", params);
        execute("CALL apoc.index.addAllNodes('hackerz', {Hacker:['name']})");
        execute("MATCH (h:Hacker {name:{name}}) DELETE h", map("name",john));
        // expect
        TestUtil.testResult(db, "CALL apoc.index.search('hackerz', 'D*') YIELD node, weight " +
                "RETURN node.name AS name", result -> {
            List<Object> names = Iterators.asList(result.columnAs("name"));
            assertTrue(names.contains(jim));
        });
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

    @Test
    public void addAllNodesDefaultParameters() {
        execute("CALL apoc.index.addAllNodes('hackerz', {Hacker:['name']})");

        TestUtil.testResult( db, "CALL apoc.index.list() yield config", result -> {
            List<Object> configs = Iterators.asList( result.columnAs("config") );
            assertEquals( 1, configs.size() );
            Map<String,Object> config = (Map) configs.get( 0 );
            assertEquals( 5, config.size() ); // expecting 5 values

            // 5 default values
            assertEquals( config.get("type"), "fulltext" );
            assertEquals( config.get("to_lower_case"), "true" );
            assertEquals( config.get("provider"), "lucene" );
            assertEquals( config.get("keysForLabel:Hacker"), "name" );
            assertEquals( config.get("labels"), "Hacker" );

        });
    }

    @Test
    public void addAllNodesExtendedNoOptionsDefaultParameters() {
        execute("CALL apoc.index.addAllNodesExtended('hackerz', {Hacker:['name']},{})"); // note the ,{} here

        TestUtil.testResult( db, "CALL apoc.index.list() yield config", result -> {
            List<Object> configs = Iterators.asList( result.columnAs("config") );
            assertEquals( 1, configs.size() );
            Map<String,Object> config = (Map) configs.get( 0 );
            assertEquals( 5, config.size() ); // expecting 5 values

            // 5 default values
            assertEquals( config.get("type"), "fulltext" );
            assertEquals( config.get("to_lower_case"), "true" );
            assertEquals( config.get("provider"), "lucene" );
            assertEquals( config.get("keysForLabel:Hacker"), "name" );
            assertEquals( config.get("labels"), "Hacker" );

        });
    }

    @Test
    public void addAllNodesExtendedOptionsCheck() throws Exception {
        execute("CALL apoc.index.addAllNodesExtended('hackerz', {Hacker:['name']},{autoUpdate: true, to_lower_case: false, foo: \"bar\"})");

        TestUtil.testResult( db, "CALL apoc.index.list() yield config", result -> {
            List<Object> configs = Iterators.asList( result.columnAs("config") );
            assertEquals( 1, configs.size() );
            Map<String,Object> config = (Map) configs.get( 0 );
            assertEquals( 7, config.size() ); // expecting 5 values

            // 5 default values
            assertEquals( config.get("type"), "fulltext" );
            assertEquals( config.get("to_lower_case"), "false" ); // overridden to false
            assertEquals( config.get("autoUpdate"), "true" ); // additional
            assertEquals( config.get("provider"), "lucene" );
            assertEquals( config.get("keysForLabel:Hacker"), "name" );
            assertEquals( config.get("labels"), "Hacker" );
            assertEquals( config.get("foo"), "bar" ); // additional
        });
    }

    @Test
    public void addAllNodesExtendedOptionsCustomAnalyzer() throws Exception {
        execute("CALL apoc.index.addAllNodesExtended('hackerz', {Hacker:['name']},{analyzer: \"apoc.index.analyzer.DynamicChainAnalyzer\"})");

        TestUtil.testResult( db, "CALL apoc.index.list() yield config", result -> {
            List<Object> configs = Iterators.asList( result.columnAs("config") );
            assertEquals( 1, configs.size() );
            Map<String,Object> config = (Map) configs.get( 0 );
            assertEquals( 6, config.size() ); // expecting 5 values

            // 5 default values
            assertEquals( config.get("type"), "fulltext" );
            assertEquals( config.get("to_lower_case"), "true" ); // overridden to false
            assertEquals( config.get("provider"), "lucene" );
            assertEquals( config.get("keysForLabel:Hacker"), "name" );
            assertEquals( config.get("labels"), "Hacker" );
            assertEquals( config.get("analyzer"), DynamicChainAnalyzer.class.getName() );
        });
    }


    @Test
    public void addAllNodeExtendedWithDynamicChainAnalyzer() throws Exception {
        // given
        String john = "John Doe";
        String jim = "Jim Done";
        String fred = "Fred Finished";
        Map params = singletonMap("names", Iterators.array(john, jim, fred));
        execute("UNWIND {names} as name CREATE (:Hacker{name:name})", params);
        execute(
            "CALL apoc.index.addAllNodesExtended('hackerz', {Hacker:['name']},{analyzer: \"apoc.index.analyzer.DynamicChainAnalyzer\"})");

        // expect
        TestUtil.testResult(db, "CALL apoc.index.search('hackerz', 'D*') yield node, weight "
            + "RETURN node.name as name", result -> {
            List<Object> names = Iterators.asList(result.columnAs("name"));
            assertTrue(names.contains(jim));
            assertTrue(names.contains(john));
            assertFalse(names.contains(fred));
        });

        // expect
        TestUtil.testResult(db, "CALL apoc.index.search('hackerz', '*shed') yield node, weight "
            + "RETURN node.name as name", result -> {
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
                boolean hasLabel;
                try (Transaction tx = item.getGraphDatabase().beginTx()) {
                    hasLabel = item.hasLabel(label(label));
                    tx.success();
                }
                if (hasLabel) {
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
                Object property;
                try (Transaction tx = item.getGraphDatabase().beginTx()) {
                    property = item.getProperty(key, null);
                    tx.success();
                }
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
