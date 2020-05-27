package apoc.nlp.aws

import apoc.nlp.NodeMatcher
import apoc.nlp.RelationshipMatcher
import apoc.result.VirtualNode
import apoc.util.TestUtil
import junit.framework.Assert.assertEquals
import junit.framework.Assert.assertTrue
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.*
import org.junit.Assume.assumeTrue
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Test
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.test.rule.ImpermanentDbmsRule


class AWSProceduresAPITest {
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

        val apiKey: String? = System.getenv("AWS_API_KEY")
        val apiSecret: String? = System.getenv("AWS_API_SECRET")

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

        fun nodeMatches(item: Node?, labels: List<String>?, properties: Map<String, Any>?): Boolean {
            val labelsMatched = item?.labels!!.count() == labels?.size  && item.labels!!.all { label -> labels?.contains(label.name()) }
            val propertiesMatches = propertiesMatch(properties, item.allProperties)
            return labelsMatched && propertiesMatches
        }

        fun propertiesMatch(expected: Map<String, Any>?, actual: Map<String, Any>?) =
                actual?.keys == expected?.keys && actual!!.all { entry -> expected?.containsKey(entry.key)!! && expected[entry.key] == entry.value }
    }

    @Test
    fun `should extract entities for individual nodes`() {
        neo4j.executeTransactionally("""CREATE (a:Article {body:${'$'}body, id: 1})""", mapOf("body" to article1))
        neo4j.executeTransactionally("""CREATE (a:Article {body:${'$'}body, id: 2})""", mapOf("body" to article2))

        neo4j.executeTransactionally("""
                    MATCH (a:Article) WITH a ORDER BY a.id
                    CALL apoc.nlp.aws.entities.stream(a, {
                      key: ${'$'}apiKey,
                      secret: ${'$'}apiSecret,
                      nodeProperty: "body"
                    })
                    YIELD value
                    RETURN value
                """.trimIndent(), mapOf("apiKey" to apiKey, "apiSecret" to apiSecret)) {
            val row1 = it.next()
            val value = row1["value"] as Map<*, *>
            val entities = value["entities"] as List<*>

            assertThat(entities, hasItem(mapOf("beginOffset" to 80L, "endOffset" to 83L, "score" to 0.99441046F, "text" to  "NHS", "type" to  "ORGANIZATION")))
            assertThat(entities, hasItem(mapOf("beginOffset" to 93L, "endOffset" to 100L, "score" to 0.99799734F, "text" to  "England", "type" to  "LOCATION")))
            assertThat(entities, hasItem(mapOf("beginOffset" to 118L, "endOffset" to 134L, "score" to 0.9790507F, "text" to  "Health Secretary", "type" to  "PERSON")))
            assertThat(entities, hasItem(mapOf("beginOffset" to 135L, "endOffset" to 147L, "score" to 0.9745345F, "text" to  "Matt Hancock", "type" to  "PERSON")))
            assertThat(entities, hasItem(mapOf("beginOffset" to 321L, "endOffset" to 328L, "score" to  0.99982184F, "text" to  "Tuesday", "type" to  "DATE")))
            assertThat(entities, hasItem(mapOf("beginOffset" to 354L, "endOffset" to 366L, "score" to 0.9981365F, "text" to  "Michael Gove", "type" to  "PERSON")))
            assertThat(entities, hasItem(mapOf("beginOffset" to 380L, "endOffset" to 382L, "score" to  0.8772306F, "text" to  "UK", "type" to  "ORGANIZATION")))

            assertTrue(it.hasNext())

            val row2 = it.next()
            val value2 = row2["value"] as Map<*, *>
            val entities2 = value2["entities"] as List<*>

            assertThat(entities2, hasItem(mapOf("beginOffset" to 13L, "endOffset" to 25L, "score" to 0.9996571F, "text" to  "Leeds United", "type" to  "ORGANIZATION")))
            assertThat(entities2, hasItem(mapOf("beginOffset" to 32L, "endOffset" to 45L, "score" to 0.99996364F, "text" to  "Norman Hunter", "type" to  "PERSON")))
            assertThat(entities2, hasItem(mapOf("beginOffset" to 67L, "endOffset" to 74L, "score" to 0.90416294F, "text" to  "aged 76", "type" to  "QUANTITY")))
            assertThat(entities2, hasItem(mapOf("beginOffset" to 161L, "endOffset" to 176L, "score" to 0.9958226F, "text" to "Bites Yer Legs'", "type" to "PERSON")))
            assertThat(entities2, hasItem(mapOf("beginOffset" to 198L, "endOffset" to 204L, "score" to 0.97942513F, "text" to "Leeds'", "type" to "ORGANIZATION")))
            assertThat(entities2, hasItem(mapOf("beginOffset" to 245L, "endOffset" to 262L, "score" to 0.9205728F, "text" to "two league titles", "type" to "QUANTITY")))
            assertThat(entities2, hasItem(mapOf("beginOffset" to 272L, "endOffset" to 279L, "score" to 0.99927694F, "text" to "14-year", "type" to "QUANTITY")))
            assertThat(entities2, hasItem(mapOf("beginOffset" to 301L, "endOffset" to 312L, "score" to 0.99773335F, "text" to "Elland Road", "type" to "LOCATION")))
            assertThat(entities2, hasItem(mapOf("beginOffset" to 346L, "endOffset" to 353L, "score" to 0.8237946F, "text" to "England", "type" to "LOCATION")))
            assertThat(entities2, hasItem(mapOf("beginOffset" to 356L, "endOffset" to 370L, "score" to 0.8252391F, "text" to "1966 World Cup", "type" to "EVENT")))
            assertThat(entities2, hasItem(mapOf("beginOffset" to 398L, "endOffset" to 404L, "score" to 0.994545F, "text" to "Hunter", "type" to "PERSON")))
            assertThat(entities2, hasItem(mapOf("beginOffset" to 433L, "endOffset" to 441L, "score" to 0.99983764F, "text" to "10 April", "type" to "DATE")))
        }
    }

    @Test
    fun `should extract entities for collection of nodes`() {
        neo4j.executeTransactionally("""CREATE (a:Article2 {body:${'$'}body, id: 1})""", mapOf("body" to article1))
        neo4j.executeTransactionally("""CREATE (a:Article2 {body:${'$'}body, id: 2})""", mapOf("body" to article2))

        neo4j.executeTransactionally("""
                    MATCH (a:Article2)
                    WITH a ORDER BY a.id
                    WITH collect(a) AS articles
                    CALL apoc.nlp.aws.entities.stream(articles, {
                      key: ${'$'}apiKey,
                      secret: ${'$'}apiSecret,
                      nodeProperty: "body"
                    })
                    YIELD node, value, error
                    RETURN node, value, error
                """.trimIndent(), mapOf("apiKey" to apiKey, "apiSecret" to apiSecret)) {
            assertTrue(it.hasNext())

            val row1 = it.next()
            val value = row1["value"] as Map<*, *>
            val entities = value["entities"] as List<*>

            assertThat(entities, hasItem(mapOf("beginOffset" to 80L, "endOffset" to 83L, "score" to 0.99441046F, "text" to  "NHS", "type" to  "ORGANIZATION")))
            assertThat(entities, hasItem(mapOf("beginOffset" to 93L, "endOffset" to 100L, "score" to 0.99799734F, "text" to  "England", "type" to  "LOCATION")))
            assertThat(entities, hasItem(mapOf("beginOffset" to 118L, "endOffset" to 134L, "score" to 0.9790507F, "text" to  "Health Secretary", "type" to  "PERSON")))
            assertThat(entities, hasItem(mapOf("beginOffset" to 135L, "endOffset" to 147L, "score" to 0.9745345F, "text" to  "Matt Hancock", "type" to  "PERSON")))
            assertThat(entities, hasItem(mapOf("beginOffset" to 321L, "endOffset" to 328L, "score" to  0.99982184F, "text" to  "Tuesday", "type" to  "DATE")))
            assertThat(entities, hasItem(mapOf("beginOffset" to 354L, "endOffset" to 366L, "score" to 0.9981365F, "text" to  "Michael Gove", "type" to  "PERSON")))
            assertThat(entities, hasItem(mapOf("beginOffset" to 380L, "endOffset" to 382L, "score" to  0.8772306F, "text" to  "UK", "type" to  "ORGANIZATION")))

            assertTrue(it.hasNext())

            val row2 = it.next()
            val value2 = row2["value"] as Map<*, *>
            val entities2 = value2["entities"] as List<*>

            assertThat(entities2, hasItem(mapOf("beginOffset" to 13L, "endOffset" to 25L, "score" to 0.9996571F, "text" to  "Leeds United", "type" to  "ORGANIZATION")))
            assertThat(entities2, hasItem(mapOf("beginOffset" to 32L, "endOffset" to 45L, "score" to 0.99996364F, "text" to  "Norman Hunter", "type" to  "PERSON")))
            assertThat(entities2, hasItem(mapOf("beginOffset" to 67L, "endOffset" to 74L, "score" to 0.90416294F, "text" to  "aged 76", "type" to  "QUANTITY")))
            assertThat(entities2, hasItem(mapOf("beginOffset" to 161L, "endOffset" to 176L, "score" to 0.9958226F, "text" to "Bites Yer Legs'", "type" to "PERSON")))
            assertThat(entities2, hasItem(mapOf("beginOffset" to 198L, "endOffset" to 204L, "score" to 0.97942513F, "text" to "Leeds'", "type" to "ORGANIZATION")))
            assertThat(entities2, hasItem(mapOf("beginOffset" to 245L, "endOffset" to 262L, "score" to 0.9205728F, "text" to "two league titles", "type" to "QUANTITY")))
            assertThat(entities2, hasItem(mapOf("beginOffset" to 272L, "endOffset" to 279L, "score" to 0.99927694F, "text" to "14-year", "type" to "QUANTITY")))
            assertThat(entities2, hasItem(mapOf("beginOffset" to 301L, "endOffset" to 312L, "score" to 0.99773335F, "text" to "Elland Road", "type" to "LOCATION")))
            assertThat(entities2, hasItem(mapOf("beginOffset" to 346L, "endOffset" to 353L, "score" to 0.8237946F, "text" to "England", "type" to "LOCATION")))
            assertThat(entities2, hasItem(mapOf("beginOffset" to 356L, "endOffset" to 370L, "score" to 0.8252391F, "text" to "1966 World Cup", "type" to "EVENT")))
            assertThat(entities2, hasItem(mapOf("beginOffset" to 398L, "endOffset" to 404L, "score" to 0.994545F, "text" to "Hunter", "type" to "PERSON")))
            assertThat(entities2, hasItem(mapOf("beginOffset" to 433L, "endOffset" to 441L, "score" to 0.99983764F, "text" to "10 April", "type" to "DATE")))
        }
    }

    @Test
    fun `should extract entity as virtual graph`() {
        neo4j.executeTransactionally("""CREATE (a:Article3 {id: 1234, body:${'$'}body})""", mapOf("body" to article1))

        var sourceNode: Node? = null
        var virtualSourceNode: Node? = null
        neo4j.executeTransactionally("MATCH (a:Article3) RETURN a", emptyMap()) {
            sourceNode = it.next()["a"] as Node
            virtualSourceNode = VirtualNode(sourceNode, sourceNode!!.propertyKeys.toList())
        }

        neo4j.executeTransactionally("""
                    MATCH (a:Article3)
                    CALL apoc.nlp.aws.entities.graph(a, {
                      key: ${'$'}apiKey,
                      secret: ${'$'}apiSecret,
                      nodeProperty: "body",
                      write: false
                    })
                    YIELD graph AS g
                    RETURN g.nodes AS nodes, g.relationships AS relationships
                """.trimIndent(), mapOf("apiKey" to apiKey, "apiSecret" to apiSecret)) {
            assertTrue(it.hasNext())

            it.forEach { row ->
                run {
                    val nodes: List<Node> = row["nodes"] as List<Node>
                    val relationships = row["relationships"] as List<Relationship>

                    assertEquals(8, nodes.size)

                    assertThat(nodes, hasItem(sourceNode))

                    val orgLabels = listOf(Label { "Organization" }, Label { "Entity" })
                    val locationLabels = listOf(Label { "Location" }, Label { "Entity" })
                    val personLabels = listOf(Label { "Person" }, Label { "Entity" })
                    val dateLabels = listOf(Label { "Date" }, Label { "Entity" })

                    assertThat(nodes, hasItem(NodeMatcher(orgLabels, mapOf("text" to "NHS", "type" to "ORGANIZATION"))))
                    assertThat(nodes, hasItem(NodeMatcher(orgLabels, mapOf("text" to "UK", "type" to "ORGANIZATION"))))

                    assertThat(nodes, hasItem(NodeMatcher(locationLabels, mapOf("text" to "England", "type" to "LOCATION"))))

                    assertThat(nodes, hasItem(NodeMatcher(personLabels, mapOf("text" to "Health Secretary", "type" to "PERSON"))))
                    assertThat(nodes, hasItem(NodeMatcher(personLabels, mapOf("text" to "Matt Hancock", "type" to "PERSON"))))
                    assertThat(nodes, hasItem(NodeMatcher(personLabels, mapOf("text" to "Michael Gove", "type" to "PERSON"))))

                    assertThat(nodes, hasItem(NodeMatcher(dateLabels, mapOf("text" to "Tuesday", "type" to "DATE"))))

                    assertEquals(7, relationships.size)

                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(orgLabels.toTypedArray(), mapOf("text" to "NHS", "type" to "ORGANIZATION")), "ENTITY", mapOf("score" to 0.99441046F))))
                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(orgLabels.toTypedArray(), mapOf("text" to "UK", "type" to "ORGANIZATION")), "ENTITY", mapOf("score" to 0.8772306F))))
                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(locationLabels.toTypedArray(), mapOf("text" to "England", "type" to "LOCATION")), "ENTITY", mapOf("score" to 0.99799734F))))
                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(personLabels.toTypedArray(), mapOf("text" to "Health Secretary", "type" to "PERSON")), "ENTITY", mapOf("score" to 0.9790507F))))
                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(personLabels.toTypedArray(), mapOf("text" to "Matt Hancock", "type" to "PERSON")), "ENTITY", mapOf("score" to 0.9745345F))))
                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(personLabels.toTypedArray(), mapOf("text" to "Michael Gove", "type" to "PERSON")), "ENTITY", mapOf("score" to 0.9981365F))))
                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(dateLabels.toTypedArray(), mapOf("text" to "Tuesday", "type" to "DATE")), "ENTITY", mapOf("score" to 0.99982184F))))
                }
            }
        }
    }
}

