package apoc.nlp.aws

import apoc.result.VirtualNode
import apoc.util.TestUtil
import junit.framework.Assert.assertEquals
import junit.framework.Assert.assertTrue
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.hasItem
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

        fun nodeMatches(item: Node?, labels: List<String>?, properties: Map<String, Any>?): Boolean {
            val labelsMatched = item?.labels!!.all { label -> labels?.contains(label.name())!! }
            val propertiesMatches = item.allProperties.all { entry -> properties?.containsKey(entry.key)!! && properties.get(entry.key) == entry.value }
            return labelsMatched && propertiesMatches
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

    @Test
    fun `should extract entity as virtual graph`() {
        neo4j.executeTransactionally("""CREATE (a:Article {id: 1234, body:${'$'}body})""", mapOf("body" to article1))

        var sourceNode: Node? = null
        var virtualSourceNode: Node? = null
        neo4j.executeTransactionally("MATCH (a:Article) RETURN a", emptyMap()) {
            sourceNode = it.next()["a"] as Node
            virtualSourceNode = VirtualNode(sourceNode, sourceNode!!.propertyKeys.toList())
        }

        neo4j.executeTransactionally("""
                    MATCH (a:Article)
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

                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(orgLabels.toTypedArray(), mapOf("text" to "NHS", "type" to "ORGANIZATION")), "ENTITY")))
                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(orgLabels.toTypedArray(), mapOf("text" to "UK", "type" to "ORGANIZATION")), "ENTITY")))
                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(locationLabels.toTypedArray(), mapOf("text" to "England", "type" to "LOCATION")), "ENTITY")))
                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(personLabels.toTypedArray(), mapOf("text" to "Health Secretary", "type" to "PERSON")), "ENTITY")))
                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(personLabels.toTypedArray(), mapOf("text" to "Matt Hancock", "type" to "PERSON")), "ENTITY")))
                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(personLabels.toTypedArray(), mapOf("text" to "Michael Gove", "type" to "PERSON")), "ENTITY")))
                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(dateLabels.toTypedArray(), mapOf("text" to "Tuesday", "type" to "DATE")), "ENTITY")))
                }
            }
        }
    }


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

