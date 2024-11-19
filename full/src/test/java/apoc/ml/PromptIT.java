package apoc.ml;

import static apoc.util.TestUtil.testResult;

import apoc.coll.Coll;
import apoc.meta.Meta;
import apoc.text.Strings;
import apoc.util.TestUtil;
import apoc.util.Util;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static apoc.util.TestUtil.testCall;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class PromptIT {

    private static final String OPENAI_KEY = System.getenv("OPENAI_KEY");

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void check() {
        Assume.assumeNotNull("No OPENAI_KEY environment configured", OPENAI_KEY);
    }

    @Before
    public void setUp() {
        TestUtil.registerProcedure(db, Prompt.class, Meta.class, Strings.class, Coll.class);
        String movies = Util.readResourceFile("movies.cypher");
        try (Transaction tx = db.beginTx()) {
            tx.execute(movies);
            tx.commit();
        }
    }

    @After
    public void teardown() {
        db.shutdown();
    }

    @Test
    public void testQuery() {
        testResult(
                db,
                "CALL apoc.ml.query($query, {retries: $retries, apiKey: $apiKey})",
                Map.of("query", "What movies did Tom Hanks play in?", "retries", 2L, "apiKey", OPENAI_KEY),
                (r) -> {
                    List<Map<String, Object>> list = r.stream().collect(Collectors.toList());
                    assertThat(list).hasSize(12);
                    assertThat(list.stream()
                                    .map(m -> m.get("query"))
                                    .filter(Objects::nonNull)
                                    .map(Object::toString)
                                    .map(String::trim))
                            .isNotEmpty();
                });
    }

    @Test
    public void testSchema() {
        testResult(db, "CALL apoc.ml.schema({apiKey: $apiKey})", Map.of("apiKey", OPENAI_KEY), (r) -> {
            List<Map<String, Object>> list = r.stream().collect(Collectors.toList());
            assertThat(list).hasSize(1);
        });
    }

    @Test
    public void testCypher() {
        long numOfQueries = 4L;
        testResult(
                db,
                "CALL apoc.ml.cypher($query, {count: $numOfQueries, apiKey: $apiKey})",
                Map.of(
                        "query", "Who are the actors which also directed a movie?",
                        "numOfQueries", numOfQueries,
                        "apiKey", OPENAI_KEY),
                (r) -> {
                    List<Map<String, Object>> list = r.stream().collect(Collectors.toList());
                    assertThat(list).hasSize((int) numOfQueries);
                    assertThat(list.stream()
                                    .map(m -> m.get("query"))
                                    .filter(Objects::nonNull)
                                    .map(Object::toString)
                                    .filter(StringUtils::isNotEmpty))
                            .hasSize((int) numOfQueries);
                });
    }

    @Test
    public void testSchemaFromQueries() {
        List<String> queries = List.of("MATCH p=(n:Movie)--() RETURN p", "MATCH (n:Person) RETURN n", "MATCH (n:Movie) RETURN n", "MATCH p=(n)-[r]->() RETURN r");

        testCall(db, "CALL apoc.ml.fromQueries($queries, {apiKey: $apiKey})",
                Map.of(
                        "queries", queries,
                        "apiKey", OPENAI_KEY
                ),
                (r) -> {

                    String value = ((String) r.get("value")).toLowerCase();
                    assertThat(value).containsIgnoringCase("movie");
                    assertThat(value).satisfiesAnyOf(s -> assertThat(s).contains("person"),
                                    s -> assertThat(s).contains("people"));
                });
    }

    @Test
    public void testSchemaFromQueriesWithSingleQuery() {
        List<String> queries = List.of("MATCH (n:Movie) RETURN n");

        testCall(db, "CALL apoc.ml.fromQueries($queries, {apiKey: $apiKey})",
                Map.of(
                        "queries", queries,
                        "apiKey", OPENAI_KEY
                ),
                (r) -> {
                    String value = ((String) r.get("value")).toLowerCase();
                    assertThat(value).containsIgnoringCase("movie");
                    assertThat(value).doesNotContainIgnoringCase("person", "people");
                });
    }

    @Test
    public void testSchemaFromQueriesWithWrongQuery() {
        List<String> queries = List.of("MATCH (n:Movie) RETURN a");
        try {
            testCall(db, "CALL apoc.ml.fromQueries($queries, {apiKey: $apiKey})",
                    Map.of(
                            "queries", queries,
                            "apiKey", OPENAI_KEY
                    ),
                    (r) -> fail());
        } catch (Exception e) {
            assertThat(e.getMessage()).contains(" Variable `a` not defined");
        }

    }

    @Test
    public void testSchemaFromEmptyQueries() {
        List<String> queries = List.of("MATCH (n:Movie) RETURN 1");

        testCall(db, "CALL apoc.ml.fromQueries($queries, {apiKey: $apiKey})",
                Map.of(
                        "queries", queries,
                        "apiKey", OPENAI_KEY
                ),
                (r) -> {
                    String value = ((String) r.get("value")).toLowerCase();

                    assertThat(value).satisfiesAnyOf(s -> assertThat(s).contains("does not contain"),
                            s -> assertThat(s).contains("empty"),
                            s -> assertThat(s).contains("undefined"),
                            s -> assertThat(s).contains("doesn't have")
                    );
                });
    }
}
