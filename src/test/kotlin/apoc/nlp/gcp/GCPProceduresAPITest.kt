package apoc.nlp.gcp

import apoc.nlp.MinimalPropertiesMatcher.Companion.hasAtLeast
import apoc.util.TestUtil
import org.hamcrest.MatcherAssert
import org.hamcrest.Matchers
import org.junit.Assume.assumeTrue
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Test
import org.neo4j.test.rule.ImpermanentDbmsRule

class GCPProceduresAPITest {
    companion object {
        const val body = """
            Hospitals should use spare laboratory space to test self-isolating NHS staff in England for coronavirus, Health Secretary Matt Hancock has said.
            The government faces growing criticism over a lack of testing for frontline staff who could return to work if found clear of the virus.
            On Tuesday, Cabinet Office minister Michael Gove admitted the UK had to go "further, faster" to increase testing.
        """

        val apiKey: String? = System.getenv("GCP_API_KEY")

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

        neo4j.executeTransactionally("""
                    MATCH (a:Article)
                    CALL apoc.nlp.gcp.entities.stream(a, {
                      key: ${'$'}apiKey,
                      nodeProperty: "body"
                    })
                    YIELD value
                    RETURN value
                """.trimIndent(), mapOf("apiKey" to apiKey)) {
            val row1 = it.next()
            val value = row1["value"] as Map<*, *>
            val entities = value["entities"] as List<*>

            MatcherAssert.assertThat(entities, Matchers.hasItem(hasAtLeast(mapOf("salience" to 0.17007294, "name" to "testing", "type" to "OTHER"))))
            MatcherAssert.assertThat(entities, Matchers.hasItem(hasAtLeast(mapOf("salience" to 0.16034527, "name" to "laboratory space", "type" to "OTHER"))))
            MatcherAssert.assertThat(entities, Matchers.hasItem(hasAtLeast(mapOf("salience" to 0.12332645, "name" to "Hospitals", "type" to "ORGANIZATION"))))
            MatcherAssert.assertThat(entities, Matchers.hasItem(hasAtLeast(mapOf("salience" to 0.0887925, "name" to "Matt Hancock", "type" to "PERSON"))))
            MatcherAssert.assertThat(entities, Matchers.hasItem(hasAtLeast(mapOf("salience" to 0.086991936, "name" to "frontline staff", "type" to "PERSON"))))
            MatcherAssert.assertThat(entities, Matchers.hasItem(hasAtLeast(mapOf("salience" to 0.07244373, "name" to "staff", "type" to "PERSON"))))
            MatcherAssert.assertThat(entities, Matchers.hasItem(hasAtLeast(mapOf("salience" to 0.06391522, "name" to "coronavirus", "type" to "OTHER"))))
            MatcherAssert.assertThat(entities, Matchers.hasItem(hasAtLeast(mapOf("salience" to 0.04348713, "name" to "government", "type" to "ORGANIZATION"))))
            MatcherAssert.assertThat(entities, Matchers.hasItem(hasAtLeast(mapOf("salience" to 0.038276725, "name" to "NHS", "type" to "ORGANIZATION"))))
            MatcherAssert.assertThat(entities, Matchers.hasItem(hasAtLeast(mapOf("salience" to 0.038276725, "name" to "England", "type" to "LOCATION"))))
            MatcherAssert.assertThat(entities, Matchers.hasItem(hasAtLeast(mapOf("salience" to 0.035584744, "name" to "Michael Gove", "type" to "PERSON"))))
            MatcherAssert.assertThat(entities, Matchers.hasItem(hasAtLeast(mapOf("salience" to 0.021392597, "name" to "lack", "type" to "OTHER"))))
            MatcherAssert.assertThat(entities, Matchers.hasItem(hasAtLeast(mapOf("salience" to 0.021392597, "name" to "criticism", "type" to "OTHER"))))
            MatcherAssert.assertThat(entities, Matchers.hasItem(hasAtLeast(mapOf("salience" to 0.019643359, "name" to "work", "type" to "OTHER"))))
            MatcherAssert.assertThat(entities, Matchers.hasItem(hasAtLeast(mapOf("salience" to 0.009530788, "name" to "UK", "type" to "LOCATION"))))
            MatcherAssert.assertThat(entities, Matchers.hasItem(hasAtLeast(mapOf("salience" to 0.006527279, "name" to "virus", "type" to "OTHER"))))
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