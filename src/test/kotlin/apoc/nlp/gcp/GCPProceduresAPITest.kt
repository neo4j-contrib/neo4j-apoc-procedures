package apoc.nlp.gcp

import apoc.nlp.MinimalPropertiesMatcher.Companion.hasAtLeast
import apoc.nlp.NodeMatcher
import apoc.nlp.RelationshipMatcher
import apoc.result.VirtualNode
import apoc.util.TestUtil
import org.junit.Assert
import org.hamcrest.MatcherAssert
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers
import org.hamcrest.Matchers.hasItem
import org.junit.Assume.assumeTrue
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Test
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
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

            assertThat(entities, hasItem(hasAtLeast(mapOf("salience" to 0.17007294, "name" to "testing", "type" to "OTHER"))))
            assertThat(entities, hasItem(hasAtLeast(mapOf("salience" to 0.16034527, "name" to "laboratory space", "type" to "OTHER"))))
            assertThat(entities, hasItem(hasAtLeast(mapOf("salience" to 0.12332645, "name" to "Hospitals", "type" to "ORGANIZATION"))))
            assertThat(entities, hasItem(hasAtLeast(mapOf("salience" to 0.0887925, "name" to "Matt Hancock", "type" to "PERSON"))))
            assertThat(entities, hasItem(hasAtLeast(mapOf("salience" to 0.086991936, "name" to "frontline staff", "type" to "PERSON"))))
            assertThat(entities, hasItem(hasAtLeast(mapOf("salience" to 0.07244373, "name" to "staff", "type" to "PERSON"))))
            assertThat(entities, hasItem(hasAtLeast(mapOf("salience" to 0.06391522, "name" to "coronavirus", "type" to "OTHER"))))
            assertThat(entities, hasItem(hasAtLeast(mapOf("salience" to 0.04348713, "name" to "government", "type" to "ORGANIZATION"))))
            assertThat(entities, hasItem(hasAtLeast(mapOf("salience" to 0.038276725, "name" to "NHS", "type" to "ORGANIZATION"))))
            assertThat(entities, hasItem(hasAtLeast(mapOf("salience" to 0.038276725, "name" to "England", "type" to "LOCATION"))))
            assertThat(entities, hasItem(hasAtLeast(mapOf("salience" to 0.035584744, "name" to "Michael Gove", "type" to "PERSON"))))
            assertThat(entities, hasItem(hasAtLeast(mapOf("salience" to 0.021392597, "name" to "lack", "type" to "OTHER"))))
            assertThat(entities, hasItem(hasAtLeast(mapOf("salience" to 0.021392597, "name" to "criticism", "type" to "OTHER"))))
            assertThat(entities, hasItem(hasAtLeast(mapOf("salience" to 0.019643359, "name" to "work", "type" to "OTHER"))))
            assertThat(entities, hasItem(hasAtLeast(mapOf("salience" to 0.009530788, "name" to "UK", "type" to "LOCATION"))))
            assertThat(entities, hasItem(hasAtLeast(mapOf("salience" to 0.006527279, "name" to "virus", "type" to "OTHER"))))
        }
    }

    @Test
    fun `should extract categories`() {
        neo4j.executeTransactionally("""CREATE (a:Article2 {body:${'$'}body})""", mapOf("body" to body))

        neo4j.executeTransactionally("""
                    MATCH (a:Article2)
                    CALL apoc.nlp.gcp.classify.stream(a, {
                      key: ${'$'}apiKey,
                      nodeProperty: "body"
                    })
                    YIELD value
                    RETURN value
                """.trimIndent(), mapOf("apiKey" to apiKey)) {
            val row1 = it.next()
            val value = row1["value"] as Map<*, *>
            val entities = value["categories"] as List<*>

            assertThat(entities, hasItem(hasAtLeast(mapOf("name" to "/Health/Public Health"))))
            assertThat(entities, hasItem(hasAtLeast(mapOf("name" to "/Health/Medical Facilities & Services"))))
        }
    }

    @Test
    fun `should extract entity as virtual graph`() {
        neo4j.executeTransactionally("""CREATE (a:Article3 {body:${'$'}body})""", mapOf("body" to body))

        var sourceNode: Node? = null
        var virtualSourceNode: Node? = null
        neo4j.executeTransactionally("MATCH (a:Article3) RETURN a", emptyMap()) {
            sourceNode = it.next()["a"] as Node
            virtualSourceNode = VirtualNode(sourceNode, sourceNode!!.propertyKeys.toList())
        }

        neo4j.executeTransactionally("""
                    MATCH (a:Article3)
                    CALL apoc.nlp.gcp.entities.graph(a, {
                      key: ${'$'}apiKey,
                      nodeProperty: "body"
                    })
                    YIELD graph as g
                    RETURN g.nodes AS nodes, g.relationships AS relationships
                """.trimIndent(), mapOf("apiKey" to apiKey)) {
            Assert.assertTrue(it.hasNext())

            it.forEach { row ->
                run {
                    val nodes: List<Node> = row["nodes"] as List<Node>
                    val relationships = row["relationships"] as List<Relationship>

                    Assert.assertEquals(17, nodes.size)

                    assertThat(nodes, hasItem(sourceNode))

                    val orgLabels = listOf(Label { "Organization" }, Label { "Entity" })
                    val locationLabels = listOf(Label { "Location" }, Label { "Entity" })
                    val personLabels = listOf(Label { "Person" }, Label { "Entity" })
                    val otherLabels = listOf(Label { "Other" }, Label { "Entity" })

                    assertThat(nodes, hasItem(NodeMatcher(otherLabels, mapOf("text" to "testing", "type" to "OTHER"))))
                    assertThat(nodes, hasItem(NodeMatcher(otherLabels, mapOf("text" to "laboratory space", "type" to "OTHER"))))
                    assertThat(nodes, hasItem(NodeMatcher(otherLabels, mapOf("text" to "coronavirus", "type" to "OTHER"))))
                    assertThat(nodes, hasItem(NodeMatcher(otherLabels, mapOf("text" to "lack", "type" to "OTHER"))))
                    assertThat(nodes, hasItem(NodeMatcher(otherLabels, mapOf("text" to "criticism", "type" to "OTHER"))))
                    assertThat(nodes, hasItem(NodeMatcher(otherLabels, mapOf("text" to "work", "type" to "OTHER"))))
                    assertThat(nodes, hasItem(NodeMatcher(otherLabels, mapOf("text" to "virus", "type" to "OTHER"))))

                    assertThat(nodes, hasItem(NodeMatcher(personLabels, mapOf("text" to "Matt Hancock", "type" to "PERSON"))))
                    assertThat(nodes, hasItem(NodeMatcher(personLabels, mapOf("text" to "frontline staff", "type" to "PERSON"))))
                    assertThat(nodes, hasItem(NodeMatcher(personLabels, mapOf("text" to "staff", "type" to "PERSON"))))
                    assertThat(nodes, hasItem(NodeMatcher(personLabels, mapOf("text" to "Michael Gove", "type" to "PERSON"))))

                    assertThat(nodes, hasItem(NodeMatcher(orgLabels, mapOf("text" to "Hospitals", "type" to "ORGANIZATION"))))
                    assertThat(nodes, hasItem(NodeMatcher(orgLabels, mapOf("text" to "government", "type" to "ORGANIZATION"))))
                    assertThat(nodes, hasItem(NodeMatcher(orgLabels, mapOf("text" to "NHS", "type" to "ORGANIZATION"))))

                    assertThat(nodes, hasItem(NodeMatcher(locationLabels, mapOf("text" to "England", "type" to "LOCATION"))))
                    assertThat(nodes, hasItem(NodeMatcher(locationLabels, mapOf("text" to "UK", "type" to "LOCATION"))))

                    Assert.assertEquals(16, relationships.size)

                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(otherLabels.toTypedArray(), mapOf("text" to "testing", "type" to "OTHER")), "ENTITY", mapOf("score" to 0.17007294))))
                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(otherLabels.toTypedArray(), mapOf("text" to "laboratory space", "type" to "OTHER")), "ENTITY", mapOf("score" to 0.16034527))))
                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(otherLabels.toTypedArray(), mapOf("text" to "coronavirus", "type" to "OTHER")), "ENTITY", mapOf("score" to 0.06391522))))
                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(otherLabels.toTypedArray(), mapOf("text" to "lack", "type" to "OTHER")), "ENTITY", mapOf("score" to 0.021392597))))
                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(otherLabels.toTypedArray(), mapOf("text" to "criticism", "type" to "OTHER")), "ENTITY", mapOf("score" to 0.021392597))))
                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(otherLabels.toTypedArray(), mapOf("text" to "work", "type" to "OTHER")), "ENTITY", mapOf("score" to 0.019643359))))
                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(otherLabels.toTypedArray(), mapOf("text" to "virus", "type" to "OTHER")), "ENTITY", mapOf("score" to 0.006527279))))

                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(personLabels.toTypedArray(), mapOf("text" to "Matt Hancock", "type" to "PERSON")), "ENTITY", mapOf("score" to 0.0887925))))
                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(personLabels.toTypedArray(), mapOf("text" to "frontline staff", "type" to "PERSON")), "ENTITY", mapOf("score" to 0.086991936))))
                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(personLabels.toTypedArray(), mapOf("text" to "staff", "type" to "PERSON")), "ENTITY", mapOf("score" to 0.07244373))))
                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(personLabels.toTypedArray(), mapOf("text" to "Michael Gove", "type" to "PERSON")), "ENTITY", mapOf("score" to 0.035584744))))

                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(orgLabels.toTypedArray(), mapOf("text" to "Hospitals", "type" to "ORGANIZATION")), "ENTITY", mapOf("score" to 0.12332645))))
                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(orgLabels.toTypedArray(), mapOf("text" to "government", "type" to "ORGANIZATION")), "ENTITY", mapOf("score" to 0.04348713))))
                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(orgLabels.toTypedArray(), mapOf("text" to "NHS", "type" to "ORGANIZATION")), "ENTITY", mapOf("score" to 0.038276725))))

                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(locationLabels.toTypedArray(), mapOf("text" to "England", "type" to "LOCATION")), "ENTITY", mapOf("score" to 0.038276725))))
                    assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(locationLabels.toTypedArray(), mapOf("text" to "UK", "type" to "LOCATION")), "ENTITY", mapOf("score" to 0.009530788))))
                }
            }
        }
    }
}