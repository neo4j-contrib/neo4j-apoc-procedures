package apoc.ml;

import apoc.coll.Coll;
import apoc.meta.Meta;
import apoc.text.Strings;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.Assertions;
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

import static apoc.util.TestUtil.testResult;

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

    @Test
    public void testQuery() {
        testResult(db, """
                CALL apoc.ml.query($query, {retries: $retries, apiKey: $apiKey})
                """,
                Map.of(
                        "query", "What movies did Tom Hanks play in?",
                        "retries", 2L,
                        "apiKey", OPENAI_KEY
                ),
                (r) -> {
                    List<Map<String, Object>> list = r.stream().toList();
                    Assertions.assertThat(list).hasSize(12);
                    Assertions.assertThat(list.stream()
                                    .map(m -> m.get("query"))
                                    .filter(Objects::nonNull)
                                    .map(Object::toString)
                                    .map(String::trim))
                            .isNotEmpty();
                });
    }

    @Test
    public void testSchema() {
        testResult(db, """
                CALL apoc.ml.schema({apiKey: $apiKey})
                """,
                Map.of(
                        "apiKey", OPENAI_KEY
                ),
                (r) -> {
                    List<Map<String, Object>> list = r.stream().toList();
                    Assertions.assertThat(list).hasSize(1);
                });
    }

    @Test
    public void testCypher() {
        long numOfQueries = 4L;
        testResult(db, """
                CALL apoc.ml.cypher($query, {count: $numOfQueries, apiKey: $apiKey})
                """,
                Map.of(
                        "query", "Who are the actors which also directed a movie?",
                        "numOfQueries", numOfQueries,
                        "apiKey", OPENAI_KEY
                ),
                (r) -> {
                    List<Map<String, Object>> list = r.stream().toList();
                    Assertions.assertThat(list).hasSize((int) numOfQueries);
                    Assertions.assertThat(list.stream()
                                    .map(m -> m.get("query"))
                                    .filter(Objects::nonNull)
                                    .map(Object::toString)
                                    .filter(StringUtils::isNotEmpty))
                            .hasSize((int) numOfQueries);
                });
    }

}
