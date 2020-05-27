package apoc.nlp.gcp

import apoc.nlp.NodeMatcher
import apoc.nlp.RelationshipMatcher
import apoc.result.VirtualNode
import apoc.util.TestUtil
import org.junit.Assert
import org.junit.Assert.assertTrue
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
import java.util.stream.Collectors


class GCPProceduresAPIWithDummyClientTest {
    companion object {
        val apiKey: String? = "dummyKey"

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
        neo4j.executeTransactionally("""CREATE (a:Article {body:${'$'}body, id: 1})""", mapOf("body" to "dummyText"))
        neo4j.executeTransactionally("""CREATE (a:Article {body:${'$'}body, id: 2})""", mapOf("body" to "dummyText"))

        neo4j.executeTransactionally("""
                    MATCH (a:Article) WITH a ORDER BY a.id
                    WITH collect(a) AS articles
                    CALL apoc.nlp.gcp.entities.stream(articles, {
                      key: ${'$'}apiKey,
                      nodeProperty: "body",
                      unsupportedDummyClient: true
                    })
                    YIELD value
                    RETURN value
                """.trimIndent(), mapOf("apiKey" to apiKey)) {
            val row1 = it.next()
            val value = row1["value"] as Map<*, *>
            val entities = value["entities"] as List<*>

            assertThat(entities, hasItem(mapOf("salience" to 0.1, "name" to "token-1-index-0-batch-0", "type" to "CONSUMER_GOOD")))
            assertThat(entities, hasItem(mapOf("salience" to 0.2, "name" to "token-2-index-0-batch-0", "type" to "LOCATION")))

            assertTrue(it.hasNext())

            val row2 = it.next()
            val value2 = row2["value"] as Map<*, *>
            val entities2 = value2["entities"] as List<*>

            assertThat(entities2, hasItem(mapOf("salience" to 0.1, "name" to "token-1-index-1-batch-0", "type" to "CONSUMER_GOOD")))
            assertThat(entities2, hasItem(mapOf("salience" to 0.2, "name" to "token-2-index-1-batch-0", "type" to "LOCATION")))
        }
    }

    @Test
    fun `should extract in batches`() {
        neo4j.executeTransactionally("""CREATE (a:Article2 {body:${'$'}body, id: 1})""", mapOf("body" to "dummyText"))

        neo4j.executeTransactionally("""
                    UNWIND range(1, 26) AS index
                    MATCH (a:Article2) WITH a ORDER BY a.id
                    WITH collect(a) AS articles
                    CALL apoc.nlp.gcp.entities.stream(articles, {
                      key: ${'$'}apiKey,
                      nodeProperty: "body",
                      unsupportedDummyClient: true
                    })
                    YIELD value
                    RETURN collect(value) AS values
                """.trimIndent(), mapOf("apiKey" to apiKey)) {
            val row = it.next()
            val value = row["values"] as List<*>

            val allEntities = value.stream().flatMap { v -> ((v as Map<*, *>)["entities"] as List<*>).stream() }.collect(Collectors.toList())

            // assert that we have entries from the 2nd batch
            assertThat(allEntities, hasItem(mapOf("salience" to 0.1, "name" to "token-1-index-0-batch-1", "type" to "CONSUMER_GOOD")))
            assertThat(allEntities, hasItem(mapOf("salience" to 0.2, "name" to "token-2-index-0-batch-1", "type" to "LOCATION")))
        }
    }

    @Test
    fun `batches should create multiple virtual graphs`() {
        neo4j.executeTransactionally("""CREATE (a:Article3 {id: 1234, body:${'$'}body})""", mapOf("body" to "test"))

        var sourceNode: Node? = null
        var virtualSourceNode: Node? = null
        neo4j.executeTransactionally("MATCH (a:Article3) RETURN a", emptyMap()) {
            sourceNode = it.next()["a"] as Node
            virtualSourceNode = VirtualNode(sourceNode, sourceNode!!.propertyKeys.toList())
        }

        neo4j.executeTransactionally("""
                    UNWIND range(1, 26) AS index
                    MATCH (a:Article3) WITH a ORDER BY a.id
                    WITH collect(a) AS articles
                    CALL apoc.nlp.gcp.entities.graph(articles, {
                      key: ${'$'}apiKey,
                      nodeProperty: "body",
                      unsupportedDummyClient: true
                    })
                    YIELD graph AS g
                    RETURN g.nodes AS nodes, g.relationships AS relationships
                """.trimIndent(), mapOf("apiKey" to apiKey)) {

            assertTrue(it.hasNext())
            val row1 = it.next()
            Assert.assertEquals(51, (row1["nodes"] as List<Node>).size) // 50 dummy nodes + source node

            assertTrue(it.hasNext())
            val row2 = it.next()

            val nodes: List<Node> = row2["nodes"] as List<Node>
            val relationships = row2["relationships"] as List<Relationship>
            Assert.assertEquals(3, nodes.size) // 2 dummy nodes + source node

            val dummyLabels1 = listOf(Label { "ConsumerGood"}, Label {"Entity"})
            val dummyLabels2 = listOf(Label { "Location"}, Label {"Entity"})

            assertThat(nodes, hasItem(sourceNode))
            assertThat(nodes, hasItem(NodeMatcher(dummyLabels1, mapOf("text" to "token-1-index-0-batch-1", "type" to "CONSUMER_GOOD"))))
            assertThat(nodes, hasItem(NodeMatcher(dummyLabels2, mapOf("text" to "token-2-index-0-batch-1", "type" to "LOCATION"))))

            Assert.assertEquals(2, relationships.size)
            assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(dummyLabels1.toTypedArray(), mapOf("text" to "token-1-index-0-batch-1", "type" to "CONSUMER_GOOD")), "ENTITY", mapOf("score" to 0.1))))
            assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(dummyLabels2.toTypedArray(), mapOf("text" to "token-2-index-0-batch-1", "type" to "LOCATION")), "ENTITY", mapOf("score" to 0.2))))
        }
    }

    @Test
    fun `should extract categories`() {
        neo4j.executeTransactionally("""CREATE (a:Article4 {body:${'$'}body, id: 1})""", mapOf("body" to "dummyText"))
        neo4j.executeTransactionally("""CREATE (a:Article4 {body:${'$'}body, id: 2})""", mapOf("body" to "dummyText"))

        neo4j.executeTransactionally("""
                    MATCH (a:Article4) WITH a ORDER BY a.id
                    WITH collect(a) AS articles
                    CALL apoc.nlp.gcp.classify.stream(articles, {
                      key: ${'$'}apiKey,
                      nodeProperty: "body",
                      unsupportedDummyClient: true
                    })
                    YIELD value
                    RETURN value
                """.trimIndent(), mapOf("apiKey" to apiKey)) {
            val row1 = it.next()
            val value = row1["value"] as Map<*, *>
            val categories = value["categories"] as List<*>

            assertThat(categories, hasItem(mapOf("confidence" to 0.1, "name" to "category-1-index-0-batch-0")))
            assertThat(categories, hasItem(mapOf("confidence" to 0.2, "name" to "category-2-index-0-batch-0")))

            assertTrue(it.hasNext())

            val row2 = it.next()
            val value2 = row2["value"] as Map<*, *>
            val entities2 = value2["categories"] as List<*>

            assertThat(entities2, hasItem(mapOf("confidence" to 0.1, "name" to "category-1-index-1-batch-0")))
            assertThat(entities2, hasItem(mapOf("confidence" to 0.2, "name" to "category-2-index-1-batch-0")))
        }
    }

    @Test
    fun `should extract categories in batches`() {
        neo4j.executeTransactionally("""CREATE (a:Article5 {body:${'$'}body, id: 1})""", mapOf("body" to "dummyText"))

        neo4j.executeTransactionally("""
                    UNWIND range(1, 26) AS index
                    MATCH (a:Article5) WITH a ORDER BY a.id
                    WITH collect(a) AS articles
                    CALL apoc.nlp.gcp.classify.stream(articles, {
                      key: ${'$'}apiKey,
                      nodeProperty: "body",
                      unsupportedDummyClient: true
                    })
                    YIELD value
                    RETURN collect(value) AS values
                """.trimIndent(), mapOf("apiKey" to apiKey)) {
            val row = it.next()
            val value = row["values"] as List<*>

            val allEntities = value.stream().flatMap { v -> ((v as Map<*, *>)["categories"] as List<*>).stream() }.collect(Collectors.toList())

            // assert that we have entries from the 2nd batch
            assertThat(allEntities, hasItem(mapOf("confidence" to 0.1, "name" to "category-1-index-0-batch-1")))
            assertThat(allEntities, hasItem(mapOf("confidence" to 0.2, "name" to "category-2-index-0-batch-1")))
        }
    }

    @Test
    fun `classify batches should create multiple virtual graphs`() {
        neo4j.executeTransactionally("""CREATE (a:Article6 {id: 1234, body:${'$'}body})""", mapOf("body" to "test"))

        var sourceNode: Node? = null
        var virtualSourceNode: Node? = null
        neo4j.executeTransactionally("MATCH (a:Article6) RETURN a", emptyMap()) {
            sourceNode = it.next()["a"] as Node
            virtualSourceNode = VirtualNode(sourceNode, sourceNode!!.propertyKeys.toList())
        }

        neo4j.executeTransactionally("""
                    UNWIND range(1, 26) AS index
                    MATCH (a:Article6) WITH a ORDER BY a.id
                    WITH collect(a) AS articles
                    CALL apoc.nlp.gcp.classify.graph(articles, {
                      key: ${'$'}apiKey,
                      nodeProperty: "body",
                      unsupportedDummyClient: true
                    })
                    YIELD graph AS g
                    RETURN g.nodes AS nodes, g.relationships AS relationships
                """.trimIndent(), mapOf("apiKey" to apiKey)) {

            assertTrue(it.hasNext())
            val row1 = it.next()
            Assert.assertEquals(51, (row1["nodes"] as List<Node>).size) // 50 dummy nodes + source node

            assertTrue(it.hasNext())
            val row2 = it.next()

            val nodes: List<Node> = row2["nodes"] as List<Node>
            val relationships = row2["relationships"] as List<Relationship>
            Assert.assertEquals(3, nodes.size) // 2 dummy nodes + source node

            val dummyLabels = listOf( Label {"Category"})

            assertThat(nodes, hasItem(sourceNode))
            assertThat(nodes, hasItem(NodeMatcher(dummyLabels, mapOf("text" to "category-1-index-0-batch-1"))))
            assertThat(nodes, hasItem(NodeMatcher(dummyLabels, mapOf("text" to "category-2-index-0-batch-1"))))

            Assert.assertEquals(2, relationships.size)
            assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(dummyLabels.toTypedArray(), mapOf("text" to "category-1-index-0-batch-1")), "CATEGORY", mapOf("score" to 0.1))))
            assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(dummyLabels.toTypedArray(), mapOf("text" to "category-2-index-0-batch-1")), "CATEGORY", mapOf("score" to 0.2))))
        }
    }

    @Test
    fun `create virtual entity graph based on salience cut off`() {
        neo4j.executeTransactionally("""CREATE (a:Article7 {id: 1234, body:${'$'}body})""", mapOf("body" to "test"))

        var sourceNode: Node? = null
        var virtualSourceNode: Node? = null
        neo4j.executeTransactionally("MATCH (a:Article7) RETURN a", emptyMap()) {
            sourceNode = it.next()["a"] as Node
            virtualSourceNode = VirtualNode(sourceNode, sourceNode!!.propertyKeys.toList())
        }

        neo4j.executeTransactionally("""
                    MATCH (a:Article7) WITH a ORDER BY a.id
                    WITH collect(a) AS articles
                    CALL apoc.nlp.gcp.entities.graph(articles, {
                      key: ${'$'}apiKey,
                      nodeProperty: "body",
                      unsupportedDummyClient: true,
                      scoreCutoff: 0.15,
                      writeRelationshipType: "HAS_ENTITY",
                      writeRelationshipProperty: "gcpScore"
                    })
                    YIELD graph AS g
                    RETURN g.nodes AS nodes, g.relationships AS relationships
                """.trimIndent(), mapOf("apiKey" to apiKey)) {

            assertTrue(it.hasNext())
            val row = it.next()

            val nodes: List<Node> = row["nodes"] as List<Node>
            val relationships = row["relationships"] as List<Relationship>
            Assert.assertEquals(2, nodes.size) // 1 dummy node + source node

            val dummyLabels2 = listOf(Label { "Location"}, Label {"Entity"})

            assertThat(nodes, hasItem(sourceNode))
            assertThat(nodes, hasItem(NodeMatcher(dummyLabels2, mapOf("text" to "token-2-index-0-batch-0", "type" to "LOCATION"))))

            Assert.assertEquals(1, relationships.size)
            assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(dummyLabels2.toTypedArray(), mapOf("text" to "token-2-index-0-batch-0", "type" to "LOCATION")), "HAS_ENTITY", mapOf("gcpScore" to 0.2))))
        }
    }

    @Test
    fun `create virtual category graph based on confidence cut off`() {
        neo4j.executeTransactionally("""CREATE (a:Article8 {id: 1234, body:${'$'}body})""", mapOf("body" to "test"))

        var sourceNode: Node? = null
        var virtualSourceNode: Node? = null
        neo4j.executeTransactionally("MATCH (a:Article8) RETURN a", emptyMap()) {
            sourceNode = it.next()["a"] as Node
            virtualSourceNode = VirtualNode(sourceNode, sourceNode!!.propertyKeys.toList())
        }

        neo4j.executeTransactionally("""
                    MATCH (a:Article8) WITH a ORDER BY a.id
                    WITH collect(a) AS articles
                    CALL apoc.nlp.gcp.classify.graph(articles, {
                      key: ${'$'}apiKey,
                      nodeProperty: "body",
                      unsupportedDummyClient: true,
                      scoreCutoff: 0.15 
                    })
                    YIELD graph AS g
                    RETURN g.nodes AS nodes, g.relationships AS relationships
                """.trimIndent(), mapOf("apiKey" to apiKey)) {

            assertTrue(it.hasNext())
            val row = it.next()

            val nodes: List<Node> = row["nodes"] as List<Node>
            val relationships = row["relationships"] as List<Relationship>
            Assert.assertEquals(2, nodes.size) // 1 dummy nodes + source node

            val dummyLabels = listOf( Label {"Category"})

            assertThat(nodes, hasItem(sourceNode))
            assertThat(nodes, hasItem(NodeMatcher(dummyLabels, mapOf("text" to "category-2-index-0-batch-0"))))

            Assert.assertEquals(1, relationships.size)
            assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(dummyLabels.toTypedArray(), mapOf("text" to "category-2-index-0-batch-0")), "CATEGORY", mapOf("score" to 0.2))))
        }
    }


}

