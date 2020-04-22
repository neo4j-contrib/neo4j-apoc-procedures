package apoc.nlp.gcp

import apoc.util.TestUtil
import org.junit.Assume.assumeTrue
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Test
import org.neo4j.test.rule.ImpermanentDbmsRule

class GCPProceduresTest {
    companion object {
        const val body = """
            Hospitals should use spare laboratory space to test self-isolating NHS staff in England for coronavirus, Health Secretary Matt Hancock has said.
            The government faces growing criticism over a lack of testing for frontline staff who could return to work if found clear of the virus.
            On Tuesday, Cabinet Office minister Michael Gove admitted the UK had to go "further, faster" to increase testing.
        """

        val apiKey: String? = System.getenv("API_KEY")

        @ClassRule
        @JvmField
        val neo4j = ImpermanentDbmsRule()

        @BeforeClass
        @JvmStatic
        fun beforeClass() {
            TestUtil.registerProcedure(neo4j, GCPProcedures::class.java)
            assumeTrue(apiKey != null)
        }
    }

    @Test
    fun `should extract entities`() {
        neo4j.executeTransactionally("""CREATE (a:Article {body:${'$'}body})""", mapOf("body" to body))
        neo4j.executeTransactionally("MATCH (a:Article) RETURN a", emptyMap()) {
            println(it.resultAsString())
        }
        neo4j.executeTransactionally("""
                    MATCH (a:Article)
                    CALL apoc.nlp.gcp.entities.stream(a, {
                      key: ${'$'}apiKey,
                      nodeProperty: "body"
                    })
                    YIELD value
                    RETURN value
                """.trimIndent(), mapOf("apiKey" to apiKey)) {
            println(it.resultAsString())
        }
    }

    @Test
    fun `should extract entities as virtual graph`() {
        neo4j.executeTransactionally("""CREATE (a:Article {id: 1234, body:${'$'}body})""", mapOf("body" to body))
        neo4j.executeTransactionally("MATCH (a:Article) RETURN a", emptyMap()) {
            println(it.resultAsString())
        }
        neo4j.executeTransactionally("""
                    MATCH (a:Article)
                    CALL apoc.nlp.gcp.entities.graph(a, {
                      key: ${'$'}apiKey,
                      nodeProperty: "body",
                      write: false
                    })
                    YIELD graph AS g
                    RETURN g
                """.trimIndent(), mapOf("apiKey" to apiKey)) {
            println(it.resultAsString())
        }
    }

    @Test
    fun `should extract entities as graph`() {
        neo4j.executeTransactionally("""CREATE (a:Article {id: 1234, body:${'$'}body})""", mapOf("body" to body))
        neo4j.executeTransactionally("MATCH (a:Article) RETURN a", emptyMap()) {
            println(it.resultAsString())
        }
        neo4j.executeTransactionally("""
                    MATCH (a:Article)
                    CALL apoc.nlp.gcp.entities.graph(a, {
                      key: ${'$'}apiKey,
                      nodeProperty: "body",
                      write: true
                    })
                    YIELD graph AS g
                    RETURN g
                """.trimIndent(), mapOf("apiKey" to apiKey)) {
            println(it.resultAsString())
        }
    }

    @Test
    fun `should extract categories as virtual graph`() {
        neo4j.executeTransactionally("""CREATE (a:Article {id: 1234, body:${'$'}body})""", mapOf("body" to body))
        neo4j.executeTransactionally("MATCH (a:Article) RETURN a", emptyMap()) {
            println(it.resultAsString())
        }
        neo4j.executeTransactionally("""
                    MATCH (a:Article)
                    CALL apoc.nlp.gcp.classify.graph(a, {
                      key: ${'$'}apiKey,
                      nodeProperty: "body",
                      write: false
                    })
                    YIELD graph AS g
                    RETURN g
                """.trimIndent(), mapOf("apiKey" to apiKey)) {
            println(it.resultAsString())
        }
    }
}