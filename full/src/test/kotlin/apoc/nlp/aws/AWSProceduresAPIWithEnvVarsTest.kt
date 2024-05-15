package apoc.nlp.aws

import apoc.util.TestUtil
import com.amazonaws.SDKGlobalConfiguration.ACCESS_KEY_ENV_VAR
import com.amazonaws.SDKGlobalConfiguration.SECRET_KEY_ENV_VAR
import org.junit.Assert.assertTrue
import org.junit.Assume.assumeNotNull
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Test
import org.neo4j.graphdb.Result
import org.neo4j.test.rule.ImpermanentDbmsRule


/**
 * To execute tests, set these environment variables:
 * AWS_ACCESS_KEY_ID=<apiKey>;AWS_SECRET_KEY=<secretKey>
 */
class AWSProceduresAPIWithEnvVarsTest {
    companion object {
        private val apiKey: String? = System.getenv(ACCESS_KEY_ENV_VAR)
        private val apiSecret: String? = System.getenv(SECRET_KEY_ENV_VAR)

        @ClassRule
        @JvmField
        val neo4j = ImpermanentDbmsRule()

        @BeforeClass
        @JvmStatic
        fun beforeClass() {
            neo4j.executeTransactionally("""
                CREATE (:Article {
                  uri: "https://neo4j.com/blog/pokegraph-gotta-graph-em-all/",
                  body: "These days I’m rarely more than a few feet away from my Nintendo Switch and I play board games, card games and role playing games with friends at least once or twice a week. I’ve even organised lunch-time Mario Kart 8 tournaments between the Neo4j European offices!"
                });""")
            
            neo4j.executeTransactionally("""
                CREATE (:Article {
                  uri: "https://en.wikipedia.org/wiki/Nintendo_Switch",
                  body: "The Nintendo Switch is a video game console developed by Nintendo, released worldwide in most regions on March 3, 2017. It is a hybrid console that can be used as a home console and portable device. The Nintendo Switch was unveiled on October 20, 2016. Nintendo offers a Joy-Con Wheel, a small steering wheel-like unit that a Joy-Con can slot into, allowing it to be used for racing games such as Mario Kart 8."
                });
            """)
            
            assumeNotNull(apiKey, apiSecret)
            TestUtil.registerProcedure(neo4j, AWSProcedures::class.java)
        }
    }

    @Test
    fun `should extract entities in stream mode`() {
        neo4j.executeTransactionally("""
                MATCH (a:Article {uri: "https://neo4j.com/blog/pokegraph-gotta-graph-em-all/"})
                CALL apoc.nlp.aws.entities.stream(a, {
                  nodeProperty: "body"
                })
                YIELD value
                UNWIND value.entities AS result
                RETURN result;
                """, mapOf()) {
            assertStreamWithScoreResult(it)
        }
    }

    @Test
    fun `should extract entities in graph mode`() {
        neo4j.executeTransactionally("""
                MATCH (a:Article {uri: "https://neo4j.com/blog/pokegraph-gotta-graph-em-all/"})
                CALL apoc.nlp.aws.entities.graph(a, {
                  nodeProperty: "body",
                  writeRelationshipType: "ENTITY"
                })
                YIELD graph AS g
                RETURN g;
                """, mapOf()) {
            assertGraphResult(it)
        }
    }

    @Test
    fun `should extract key phrases in stream mode`() {
        neo4j.executeTransactionally("""
                MATCH (a:Article {uri: "https://neo4j.com/blog/pokegraph-gotta-graph-em-all/"})
                CALL apoc.nlp.aws.keyPhrases.stream(a, {
                  nodeProperty: "body"
                })
                YIELD value
                UNWIND value.keyPhrases AS result
                RETURN result
                """, mapOf()) {
            assertStreamWithScoreResult(it)
        }
    }

    @Test
    fun `should extract key phrases in graph mode`() {
        neo4j.executeTransactionally("""
                MATCH (a:Article {uri: "https://neo4j.com/blog/pokegraph-gotta-graph-em-all/"})
                CALL apoc.nlp.aws.keyPhrases.graph(a, {
                  nodeProperty: "body",
                  writeRelationshipType: "KEY_PHRASE",
                  write: true
                })
                YIELD graph AS g
                RETURN g;
                """, mapOf()) {
            assertGraphResult(it)
        }
    }

    @Test
    fun `should extract sentiment in stream mode`() {
        neo4j.executeTransactionally("""
                MATCH (a:Article {uri: "https://neo4j.com/blog/pokegraph-gotta-graph-em-all/"})
                CALL apoc.nlp.aws.sentiment.stream(a, {
                  nodeProperty: "body"
                })
                YIELD value
                RETURN value AS result;
                """, mapOf()) {
            assertSentimentScoreResult(it)
        }
    }

    @Test
    fun `should extract sentiment in graph mode`() {
        neo4j.executeTransactionally("""
                MATCH (a:Article {uri: "https://neo4j.com/blog/pokegraph-gotta-graph-em-all/"})
                CALL apoc.nlp.aws.sentiment.graph(a, {
                  nodeProperty: "body",
                  write: true
                })
                YIELD graph AS g
                UNWIND g.nodes AS node
                RETURN node {.uri, .sentiment, .sentimentScore} AS result;
                """, mapOf()) {
            assertSentimentScoreResult(it)
        }
    }

    private fun assertStreamWithScoreResult(it: Result) {
        val asSequence = it.asSequence().toList()
        assertTrue(asSequence.isNotEmpty())

        asSequence.forEach {
            val entity: Map<String, Any> = it["result"] as Map<String, Any>
            assertTrue(entity.containsKey("score"))
        }
    }

    private fun assertGraphResult(it: Result) {
        val asSequence = it.asSequence().toList()
        assertTrue(asSequence.isNotEmpty())

        asSequence.forEach {
            val entity: Map<String, Any> = it["g"] as Map<String, Any>
            assertTrue(entity.containsKey("nodes"))
            assertTrue(entity.containsKey("relationships"))
        }
    }

    private fun assertSentimentScoreResult(it: Result) {
        val asSequence = it.asSequence().toList()
        assertTrue(asSequence.isNotEmpty())

        asSequence.forEach {
            val entity: Map<String, Any> = it["result"] as Map<String, Any>
            assertTrue(entity.containsKey("sentimentScore"))
        }
    }
}

