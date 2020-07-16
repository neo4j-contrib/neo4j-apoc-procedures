package apoc.nlp.azure

import apoc.nlp.NodeMatcher
import apoc.nlp.RelationshipMatcher
import apoc.nlp.aws.AWSProceduresAPIWithDummyClientTest
import apoc.result.VirtualNode
import apoc.util.TestUtil
import org.junit.Assert.assertTrue
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.hasItem
import org.junit.*
import org.junit.Assume.assumeTrue
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.ResultTransformer
import org.neo4j.test.rule.ImpermanentDbmsRule
import java.util.stream.Collectors


class AzureProceduresAPIWithDummyClientTest {
    companion object {
        val apiKey: String? = "dummyKey"
        val apiUrl: String? = "https://dummyurl"

        @ClassRule
        @JvmField
        val neo4j = ImpermanentDbmsRule()

        @BeforeClass
        @JvmStatic
        fun beforeClass() {
            TestUtil.registerProcedure(neo4j, AzureProcedures::class.java)
            assumeTrue(apiKey != null)
        }
    }

    @Test
    fun `should extract entities`() {
        var node1: Long? = null
        var node2: Long? = null
        neo4j.executeTransactionally("""CREATE (a:Article {body:${'$'}body, id: 1}) RETURN id(a) AS nodeId""", mapOf("body" to "dummyText")) {
            node1 = it.next()["nodeId"] as Long
        }
        neo4j.executeTransactionally("""CREATE (a:Article {body:${'$'}body, id: 2}) RETURN id(a) AS nodeId""", mapOf("body" to "dummyText")) {
            node2 = it.next()["nodeId"] as Long
        }

        neo4j.executeTransactionally("""
                    MATCH (a:Article) WITH a ORDER BY a.id
                    WITH collect(a) AS articles
                    CALL apoc.nlp.azure.entities.stream(articles, {
                      key: ${'$'}apiKey,
                      url: ${'$'}apiUrl,
                      nodeProperty: "body",
                      unsupportedDummyClient: true
                    })
                    YIELD value
                    RETURN value
                """.trimIndent(), mapOf("apiKey" to apiKey, "apiUrl" to apiUrl)) {
            val row1 = it.next()
            val value = row1["value"] as Map<*, *>
            val entities = value["entities"] as List<*>

            assertThat(entities, hasItem(mapOf("name" to "token-1-node-${node1}-batch-0", "type" to "Location", "matches" to listOf(mapOf("entityTypeScore" to 0.2)))))
            assertThat(entities, hasItem(mapOf("name" to "token-2-node-${node1}-batch-0", "type" to "DateTime", "matches" to listOf(mapOf("entityTypeScore" to 0.1)))))

            assertTrue(it.hasNext())

            val row2 = it.next()
            val value2 = row2["value"] as Map<*, *>
            val entities2 = value2["entities"] as List<*>

            assertThat(entities2, hasItem(mapOf("name" to "token-1-node-${node2}-batch-0", "type" to "Location", "matches" to listOf(mapOf("entityTypeScore" to 0.2)))))
            assertThat(entities2, hasItem(mapOf("name" to "token-2-node-${node2}-batch-0", "type" to "DateTime", "matches" to listOf(mapOf("entityTypeScore" to 0.1)))))
        }
    }

    @Test
    fun `should extract in batches`() {
        var node1: Long? = null
        neo4j.executeTransactionally("""CREATE (a:Article2 {body:${'$'}body, id: 1}) RETURN id(a) AS nodeId""", mapOf("body" to "dummyText")) {
            node1 = it.next()["nodeId"] as Long
        }

        neo4j.executeTransactionally("""
                    UNWIND range(1, 26) AS index
                    MATCH (a:Article2) WITH a ORDER BY a.id
                    WITH collect(a) AS articles
                    CALL apoc.nlp.azure.entities.stream(articles, {
                      key: ${'$'}apiKey,
                      url: ${'$'}apiUrl,
                      nodeProperty: "body",
                      unsupportedDummyClient: true
                    })
                    YIELD value
                    RETURN collect(value) AS values
                """.trimIndent(), mapOf("apiKey" to apiKey, "apiUrl" to apiUrl)) {
            val row = it.next()
            val value = row["values"] as List<*>

            val allEntities = value.stream().flatMap { v -> ((v as Map<*, *>)["entities"] as List<*>).stream() }.collect(Collectors.toList())

            // assert that we have entries from the 2nd batch
            assertThat(allEntities, hasItem(mapOf("name" to "token-1-node-${node1}-batch-1", "type" to "Location", "matches" to listOf(mapOf("entityTypeScore" to 0.2)))))
            assertThat(allEntities, hasItem(mapOf("name" to "token-2-node-${node1}-batch-1", "type" to "DateTime", "matches" to listOf(mapOf("entityTypeScore" to 0.1)))))
        }
    }

    @Test
    fun `batches should create multiple virtual graphs`() {
        neo4j.executeTransactionally("""UNWIND range(1,26) AS index CREATE (a:Article3 {id: index, body:${'$'}body})""", mapOf("body" to "test"))

        var sourceNode: Node? = null
        var sourceNodeId: Long? = null
        var virtualSourceNode: Node? = null
        neo4j.executeTransactionally("MATCH (a:Article3) RETURN a SKIP 25", emptyMap()) {
            sourceNode = it.next()["a"] as Node
            sourceNodeId = sourceNode!!.id
            virtualSourceNode = VirtualNode(sourceNode, sourceNode!!.propertyKeys.toList())
        }

        neo4j.executeTransactionally("""
                    MATCH (a:Article3) WITH a ORDER BY a.id
                    WITH collect(a) AS articles
                    CALL apoc.nlp.azure.entities.graph(articles, {
                      key: ${'$'}apiKey,
                      url: ${'$'}apiUrl,
                      nodeProperty: "body",
                      unsupportedDummyClient: true
                    })
                    YIELD graph AS g
                    RETURN g.nodes AS nodes, g.relationships AS relationships
                """.trimIndent(), mapOf("apiKey" to apiKey, "apiUrl" to apiUrl)) {

            assertTrue(it.hasNext())
            val row1 = it.next()
            Assert.assertEquals(75, (row1["nodes"] as List<Node>).size) // 50 dummy nodes + 25 source nodes

            assertTrue(it.hasNext())
            val row2 = it.next()

            val nodes: List<Node> = row2["nodes"] as List<Node>
            val relationships = row2["relationships"] as List<Relationship>
            Assert.assertEquals(3, nodes.size) // 2 dummy nodes + source node

            val dummyLabels1 = listOf(Label { "Location"}, Label {"Entity"})
            val dummyLabels2 = listOf(Label { "DateTime"}, Label {"Entity"})

            assertThat(nodes, hasItem(sourceNode))
            assertThat(nodes, hasItem(NodeMatcher(dummyLabels1, mapOf("text" to "token-1-node-${sourceNodeId}-batch-1", "type" to "Location"))))
            assertThat(nodes, hasItem(NodeMatcher(dummyLabels2, mapOf("text" to "token-2-node-${sourceNodeId}-batch-1", "type" to "DateTime"))))

            Assert.assertEquals(2, relationships.size)
            assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(dummyLabels1.toTypedArray(), mapOf("text" to "token-1-node-${sourceNodeId}-batch-1", "type" to "Location")), "ENTITY", mapOf("score" to 0.2))))
            assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(dummyLabels2.toTypedArray(), mapOf("text" to "token-2-node-${sourceNodeId}-batch-1", "type" to "DateTime")), "ENTITY", mapOf("score" to 0.1))))
        }
    }

    @Test
    fun `should extract key phrases`() {
        var node1: Long? = null
        var node2: Long? = null
        neo4j.executeTransactionally("""CREATE (a:Article4 {body:${'$'}body, id: 1}) RETURN id(a) AS nodeId""", mapOf("body" to "dummyText")) {
            node1 = it.next()["nodeId"] as Long
        }
        neo4j.executeTransactionally("""CREATE (a:Article4 {body:${'$'}body, id: 2}) RETURN id(a) AS nodeId""", mapOf("body" to "dummyText")) {
            node2 = it.next()["nodeId"] as Long
        }

        neo4j.executeTransactionally("""
                    MATCH (a:Article4) WITH a ORDER BY a.id
                    WITH collect(a) AS articles
                    CALL apoc.nlp.azure.keyPhrases.stream(articles, {
                      key: ${'$'}apiKey,
                      url: ${'$'}apiUrl,
                      nodeProperty: "body",
                      unsupportedDummyClient: true
                    })
                    YIELD value
                    RETURN value
                """.trimIndent(), mapOf("apiKey" to apiKey, "apiUrl" to apiUrl)) {
            val row1 = it.next()
            val value = row1["value"] as Map<*, *>
            val keyPhrases = value["keyPhrases"] as List<*>

            assertThat(keyPhrases, hasItem("keyPhrase-1-node-${node1}-batch-0"))
            assertThat(keyPhrases, hasItem("keyPhrase-2-node-${node1}-batch-0"))

            assertTrue(it.hasNext())

            val row2 = it.next()
            val value2 = row2["value"] as Map<*, *>
            val keyPhrases2 = value2["keyPhrases"] as List<*>

            assertThat(keyPhrases2, hasItem("keyPhrase-1-node-${node2}-batch-0"))
            assertThat(keyPhrases2, hasItem("keyPhrase-2-node-${node2}-batch-0"))
        }
    }

    @Test
    fun `should extract key phrases in batches`() {
        var node1: Long? = null
        neo4j.executeTransactionally("""CREATE (a:Article5 {body:${'$'}body, id: 1}) RETURN id(a) AS nodeId""", mapOf("body" to "dummyText")) {
            node1 = it.next()["nodeId"] as Long
        }

        neo4j.executeTransactionally("""
                    UNWIND range(1, 26) AS index
                    MATCH (a:Article5) WITH a ORDER BY a.id
                    WITH collect(a) AS articles
                    CALL apoc.nlp.azure.keyPhrases.stream(articles, {
                      key: ${'$'}apiKey,
                      url: ${'$'}apiUrl,
                      nodeProperty: "body",
                      unsupportedDummyClient: true
                    })
                    YIELD value
                    RETURN collect(value) AS values
                """.trimIndent(), mapOf("apiKey" to apiKey, "apiUrl" to apiUrl)) {
            val row = it.next()
            val value = row["values"] as List<*>

            val allKeyPhrases = value.stream().flatMap { v -> ((v as Map<*, *>)["keyPhrases"] as List<*>).stream() }.collect(Collectors.toList())

            // assert that we have entries from the 2nd batch
            assertThat(allKeyPhrases, hasItem("keyPhrase-1-node-${node1}-batch-1"))
            assertThat(allKeyPhrases, hasItem("keyPhrase-2-node-${node1}-batch-1"))
        }
    }

    @Test
    fun `key phrases should create multiple virtual graphs`() {
        neo4j.executeTransactionally("""UNWIND range(1, 26) AS index CREATE (a:Article6 {id: index, body:${'$'}body})""", mapOf("body" to "test"))

        var sourceNode: Node? = null
        var sourceNodeId: Long? = null
        var virtualSourceNode: Node? = null
        neo4j.executeTransactionally("MATCH (a:Article6) RETURN a SKIP 25", emptyMap()) {
            sourceNode = it.next()["a"] as Node
            sourceNodeId = sourceNode!!.id
            virtualSourceNode = VirtualNode(sourceNode, sourceNode!!.propertyKeys.toList())
        }

        neo4j.executeTransactionally("""
                    MATCH (a:Article6) WITH a ORDER BY a.id
                    WITH collect(a) AS articles
                    CALL apoc.nlp.azure.keyPhrases.graph(articles, {
                      key: ${'$'}apiKey,
                      url: ${'$'}apiUrl,
                      nodeProperty: "body",
                      unsupportedDummyClient: true
                    })
                    YIELD graph AS g
                    RETURN g.nodes AS nodes, g.relationships AS relationships
                """.trimIndent(), mapOf("apiKey" to apiKey, "apiUrl" to apiUrl)) {

            assertTrue(it.hasNext())
            val row1 = it.next()
            Assert.assertEquals(75, (row1["nodes"] as List<Node>).size) // 50 dummy nodes + 25 source nodes

            assertTrue(it.hasNext())
            val row2 = it.next()

            val nodes: List<Node> = row2["nodes"] as List<Node>
            val relationships = row2["relationships"] as List<Relationship>
            Assert.assertEquals(3, nodes.size) // 2 dummy nodes + source node

            val dummyLabels = listOf( Label {"KeyPhrase"})

            assertThat(nodes, hasItem(sourceNode))
            assertThat(nodes, hasItem(NodeMatcher(dummyLabels, mapOf("text" to "keyPhrase-1-node-${sourceNodeId}-batch-1"))))
            assertThat(nodes, hasItem(NodeMatcher(dummyLabels, mapOf("text" to "keyPhrase-2-node-${sourceNodeId}-batch-1"))))

            Assert.assertEquals(2, relationships.size)
            assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(dummyLabels.toTypedArray(), mapOf("text" to "keyPhrase-1-node-${sourceNodeId}-batch-1")), "KEY_PHRASE")))
            assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(dummyLabels.toTypedArray(), mapOf("text" to "keyPhrase-2-node-${sourceNodeId}-batch-1")), "KEY_PHRASE")))
        }
    }

    @Test
    fun `create virtual entity graph based on salience cut off`() {
        neo4j.executeTransactionally("""CREATE (a:Article7 {id: 1234, body:${'$'}body})""", mapOf("body" to "test"))

        var sourceNode: Node? = null
        var sourceNodeId: Long? = null
        var virtualSourceNode: Node? = null
        neo4j.executeTransactionally("MATCH (a:Article7) RETURN a", emptyMap()) {
            sourceNode = it.next()["a"] as Node
            sourceNodeId = sourceNode!!.id
            virtualSourceNode = VirtualNode(sourceNode, sourceNode!!.propertyKeys.toList())
        }

        neo4j.executeTransactionally("""
                    MATCH (a:Article7) WITH a ORDER BY a.id
                    WITH collect(a) AS articles
                    CALL apoc.nlp.azure.entities.graph(articles, {
                      key: ${'$'}apiKey,
                      url: ${'$'}apiUrl,
                      nodeProperty: "body",
                      unsupportedDummyClient: true,
                      scoreCutoff: 0.15,
                      writeRelationshipType: "HAS_ENTITY",
                      writeRelationshipProperty: "azureScore"
                    })
                    YIELD graph AS g
                    RETURN g.nodes AS nodes, g.relationships AS relationships
                """.trimIndent(), mapOf("apiKey" to apiKey, "apiUrl" to apiUrl)) {

            assertTrue(it.hasNext())
            val row = it.next()

            val nodes: List<Node> = row["nodes"] as List<Node>
            val relationships = row["relationships"] as List<Relationship>
            Assert.assertEquals(2, nodes.size) // 1 dummy node + source node

            val dummyLabels2 = listOf(Label { "Location"}, Label {"Entity"})

            assertThat(nodes, hasItem(sourceNode))
            assertThat(nodes, hasItem(NodeMatcher(dummyLabels2, mapOf("text" to "token-1-node-${sourceNodeId}-batch-0", "type" to "Location"))))

            Assert.assertEquals(1, relationships.size)
            assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(dummyLabels2.toTypedArray(), mapOf("text" to "token-1-node-${sourceNodeId}-batch-0", "type" to "Location")), "HAS_ENTITY", mapOf("azureScore" to 0.2))))
        }
    }

}

