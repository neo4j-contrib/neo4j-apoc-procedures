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
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Iterator;

import static apoc.index.FreeTextSearch.KEY;
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
    public void shouldQueryFreeTextIndex() throws Exception {
        // given
        execute("CREATE (:Person{name:'George Goldman', nick:'GeeGee'}), (:Person{name:'Cyrus Jones', age:103})");
        execute("CALL apoc.index.addAllNodes('people', {Person:['name','nick']})");

        // then
        assertSingle(ftsNodes("people", "GeeGee"), hasProperty("name", "George Goldman"));
        assertSingle(ftsNodes("people", "George"), hasProperty("name", "George Goldman"));
        assertSingle(ftsNodes("people", "Goldman"), hasProperty("name", "George Goldman"));
        assertSingle(ftsNodes("people", "Cyrus"), hasProperty("name", "Cyrus Jones"));
        assertSingle(ftsNodes("people", "Jones"), hasProperty("name", "Cyrus Jones"));
    }

    @Test
    public void shouldEnableSearchingBySpecificField() throws Exception {
        // given
        execute("CREATE (:Person{name:'Johnny'}), (:Product{name:'Johnny'})");
        execute("CALL apoc.index.addAllNodes('stuff', {Person:['name'],Product:['name']})");

        // then
        assertSingle(ftsNodes("stuff", "Person.name:Johnny"), hasLabel("Person"), not(hasLabel("Product")));
        assertSingle(ftsNodes("stuff", "Product.name:Johnny"), hasLabel("Product"), not(hasLabel("Person")));
    }

    @Test
    public void shouldIndexNodesWithMultipleLabels() throws Exception {
        // given
        execute("CREATE (:Foo:Bar:Baz{name:'thing'})");
        execute("CALL apoc.index.addAllNodes('stuff', {Foo:['name'], Baz:['name'], Axe:['name']})");

        // then
        assertSingle(ftsNodes("stuff", "Foo.name:thing"));
        assertSingle(ftsNodes("stuff", "Baz.name:thing"));
        assertNone(ftsNodes("stuff", "Bar.name:thing"));
        assertNone(ftsNodes("stuff", "Axe.name:thing"));
        assertSingle(ftsNodes("stuff", "thing"),
                hasLabel("Foo"), hasLabel("Bar"), hasLabel("Baz"), hasProperty("name", "thing"));
    }

    @Test
    public void shouldIndexingInBatchesWork() {
        // given
        // create 90k nodes - this force 2 batches during indexing
        execute("UNWIND range(1,90000) as x CREATE (:Person{name:'person'+x})");
        execute("CALL apoc.index.addAllNodes('people', {Person:['name']})");

        // then
        assertSingle(ftsNodes("people", "person89999"), hasProperty("name", "person89999"));
    }

    private ResourceIterator<Node> ftsNodes(String index, String value) {
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
        db.execute(query).close();
    }
}
