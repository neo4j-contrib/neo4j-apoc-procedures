package apoc.nlp.aws

import apoc.util.TestUtil
import org.junit.Assume.assumeTrue
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Test
import org.neo4j.test.rule.ImpermanentDbmsRule

class AWSProceduresTest {
    companion object {
        const val article1 = """
            Hospitals should use spare laboratory space to test self-isolating NHS staff in England for coronavirus, Health Secretary Matt Hancock has said.
            The government faces growing criticism over a lack of testing for frontline staff who could return to work if found clear of the virus.
            On Tuesday, Cabinet Office minister Michael Gove admitted the UK had to go "further, faster" to increase testing.
        """

        const val article2 = """
            Leeds United great Norman Hunter has died in hospital aged 76 after contracting coronavirus.
            The tough-tackling centre-back, nicknamed 'Bites Yer Legs', was a key player in Leeds' most successful era.
            He won two league titles during a 14-year first-team career at Elland Road, and was a non-playing member of England's 1966 World Cup-winning squad.
            Hunter was admitted to hospital on 10 April after testing positive for coronavirus.
        """

        val apiKey: String? = System.getenv("API_KEY")
        val apiSecret: String? = System.getenv("API_SECRET")

        @ClassRule
        @JvmField
        val neo4j = ImpermanentDbmsRule()

        @BeforeClass
        @JvmStatic
        fun beforeClass() {
            TestUtil.registerProcedure(neo4j, AWSProcedures::class.java)
            assumeTrue(apiKey != null)
            assumeTrue(apiSecret != null)
        }
    }

    @Test
    fun `should extract entities for individual nodes`() {
        neo4j.executeTransactionally("""CREATE (a:Article {body:${'$'}body})""", mapOf("body" to article1))
        neo4j.executeTransactionally("""CREATE (a:Article {body:${'$'}body})""", mapOf("body" to article2))
        neo4j.executeTransactionally("MATCH (a:Article) RETURN a", emptyMap()) {
            println(it.resultAsString())
        }
        neo4j.executeTransactionally("""
                    MATCH (a:Article)
                    CALL apoc.nlp.aws.entities.stream(a, {
                      key: ${'$'}apiKey,
                      secret: ${'$'}apiSecret,
                      nodeProperty: "body"
                    })
                    YIELD value
                    RETURN value
                """.trimIndent(), mapOf("apiKey" to apiKey, "apiSecret" to apiSecret)) {
            println(it.resultAsString())
        }
    }

    @Test
    fun `should extract entities for collection of nodes`() {
        neo4j.executeTransactionally("""CREATE (a:Article2 {body:${'$'}body})""", mapOf("body" to article1))
        neo4j.executeTransactionally("""CREATE (a:Article2 {body:${'$'}body})""", mapOf("body" to article2))
        neo4j.executeTransactionally("MATCH (a:Article2) RETURN a", emptyMap()) {
            println(it.resultAsString())
        }
        neo4j.executeTransactionally("""
                    MATCH (a:Article2)
                    WITH collect(a) AS articles
                    CALL apoc.nlp.aws.entities.stream(articles, {
                      key: ${'$'}apiKey,
                      secret: ${'$'}apiSecret,
                      nodeProperty: "body"
                    })
                    YIELD node, value, error
                    RETURN node, value, error
                """.trimIndent(), mapOf("apiKey" to apiKey, "apiSecret" to apiSecret)) {
            println(it.resultAsString())
        }
    }

//    @Test
//    fun `should extract entities as virtual graph`() {
//        neo4j.executeTransactionally("""CREATE (a:Article {id: 1234, body:${'$'}body})""", mapOf("body" to body))
//        neo4j.executeTransactionally("MATCH (a:Article) RETURN a", emptyMap()) {
//            println(it.resultAsString())
//        }
//        neo4j.executeTransactionally("""
//                    MATCH (a:Article)
//                    CALL apoc.nlp.aws.entities.graph(a, {
//                      key: ${'$'}apiKey,
//                      nodeProperty: "body",
//                      write: false
//                    })
//                    YIELD graph AS g
//                    RETURN g
//                """.trimIndent(), mapOf("apiKey" to apiKey)) {
//            println(it.resultAsString())
//        }
//    }
//
//    @Test
//    fun `should extract entities as graph`() {
//        neo4j.executeTransactionally("""CREATE (a:Article {id: 1234, body:${'$'}body})""", mapOf("body" to body))
//        neo4j.executeTransactionally("MATCH (a:Article) RETURN a", emptyMap()) {
//            println(it.resultAsString())
//        }
//        neo4j.executeTransactionally("""
//                    MATCH (a:Article)
//                    CALL apoc.nlp.aws.entities.graph(a, {
//                      key: ${'$'}apiKey,
//                      nodeProperty: "body",
//                      write: true
//                    })
//                    YIELD graph AS g
//                    RETURN g
//                """.trimIndent(), mapOf("apiKey" to apiKey)) {
//            println(it.resultAsString())
//        }
//    }
//
//    @Test
//    fun `should extract categories as virtual graph`() {
//        neo4j.executeTransactionally("""CREATE (a:Article {id: 1234, body:${'$'}body})""", mapOf("body" to body))
//        neo4j.executeTransactionally("MATCH (a:Article) RETURN a", emptyMap()) {
//            println(it.resultAsString())
//        }
//        neo4j.executeTransactionally("""
//                    MATCH (a:Article)
//                    CALL apoc.nlp.aws.classify.graph(a, {
//                      key: ${'$'}apiKey,
//                      nodeProperty: "body",
//                      write: false
//                    })
//                    YIELD graph AS g
//                    RETURN g
//                """.trimIndent(), mapOf("apiKey" to apiKey)) {
//            println(it.resultAsString())
//        }
//    }
}