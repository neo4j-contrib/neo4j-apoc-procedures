package apoc.nlp.aws

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


class AWSProceduresAPIWithDummyClientTest {
    companion object {
        val apiKey: String? = "dummyKey"
        val apiSecret: String? = "dummyValue"

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
    fun `should extract entities`() {
        neo4j.executeTransactionally("""CREATE (a:Article {body:${'$'}body, id: 1})""", mapOf("body" to "dummyText"))
        neo4j.executeTransactionally("""CREATE (a:Article {body:${'$'}body, id: 2})""", mapOf("body" to "dummyText"))

        neo4j.executeTransactionally("""
                    MATCH (a:Article) WITH a ORDER BY a.id
                    WITH collect(a) AS articles
                    CALL apoc.nlp.aws.entities.stream(articles, {
                      key: ${'$'}apiKey,
                      secret: ${'$'}apiSecret,
                      nodeProperty: "body",
                      unsupportedDummyClient: true
                    })
                    YIELD value
                    RETURN value
                """.trimIndent(), mapOf("apiKey" to apiKey, "apiSecret" to apiSecret)) {
            val row1 = it.next()
            val value = row1["value"] as Map<*, *>
            val entities = value["entities"] as List<*>

            assertThat(entities, hasItem(mapOf("beginOffset" to null, "endOffset" to null, "score" to null, "text" to "token-1-index-0-batch-0", "type" to "Dummy")))
            assertThat(entities, hasItem(mapOf("beginOffset" to null, "endOffset" to null, "score" to null, "text" to "token-2-index-0-batch-0", "type" to "Dummy")))

            assertTrue(it.hasNext())

            val row2 = it.next()
            val value2 = row2["value"] as Map<*, *>
            val entities2 = value2["entities"] as List<*>

            assertThat(entities2, hasItem(mapOf("beginOffset" to null, "endOffset" to null, "score" to null, "text" to "token-1-index-1-batch-0", "type" to "Dummy")))
            assertThat(entities2, hasItem(mapOf("beginOffset" to null, "endOffset" to null, "score" to null, "text" to "token-2-index-1-batch-0", "type" to "Dummy")))
        }
    }

    @Test
    fun `should extract in batches`() {
        neo4j.executeTransactionally("""CREATE (a:Article2 {body:${'$'}body, id: 1})""", mapOf("body" to "dummyText"))

        neo4j.executeTransactionally("""
                    UNWIND range(1, 26) AS index
                    MATCH (a:Article2) WITH a ORDER BY a.id
                    WITH collect(a) AS articles
                    CALL apoc.nlp.aws.entities.stream(articles, {
                      key: ${'$'}apiKey,
                      secret: ${'$'}apiSecret,
                      nodeProperty: "body",
                      unsupportedDummyClient: true
                    })
                    YIELD value
                    RETURN collect(value) AS values
                """.trimIndent(), mapOf("apiKey" to apiKey, "apiSecret" to apiSecret)) {
            val row = it.next()
            val value = row["values"] as List<*>

            val allEntities = value.stream().flatMap { v -> ((v as Map<*, *>)["entities"] as List<*>).stream() }.collect(Collectors.toList())

            // assert that we have entries from the 2nd batch
            assertThat(allEntities, hasItem(mapOf("beginOffset" to null, "endOffset" to null, "score" to null, "text" to "token-1-index-0-batch-1", "type" to "Dummy")))
            assertThat(allEntities, hasItem(mapOf("beginOffset" to null, "endOffset" to null, "score" to null, "text" to "token-2-index-0-batch-1", "type" to "Dummy")))
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
                    CALL apoc.nlp.aws.entities.graph(articles, {
                      key: ${'$'}apiKey,
                      secret: ${'$'}apiSecret,
                      nodeProperty: "body",
                      unsupportedDummyClient: true
                    })
                    YIELD graph AS g
                    RETURN g.nodes AS nodes, g.relationships AS relationships
                """.trimIndent(), mapOf("apiKey" to apiKey, "apiSecret" to apiSecret)) {

            assertTrue(it.hasNext())
            val row1 = it.next()
            Assert.assertEquals(51, (row1["nodes"] as List<Node>).size) // 50 dummy nodes + source node

            assertTrue(it.hasNext())
            val row2 = it.next()

            val nodes: List<Node> = row2["nodes"] as List<Node>
            val relationships = row2["relationships"] as List<Relationship>
            Assert.assertEquals(3, nodes.size) // 2 dummy nodes + source node

            val dummyLabels = listOf(Label { "Dummy"}, Label {"Entity"})

            assertThat(nodes, hasItem(sourceNode))
            assertThat(nodes, hasItem(NodeMatcher(dummyLabels, mapOf("text" to "token-1-index-0-batch-1", "type" to "Dummy"))))
            assertThat(nodes, hasItem(NodeMatcher(dummyLabels, mapOf("text" to "token-2-index-0-batch-1", "type" to "Dummy"))))

            Assert.assertEquals(2, relationships.size)
            assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(dummyLabels.toTypedArray(), mapOf("text" to "token-1-index-0-batch-1", "type" to "Dummy")), "ENTITY")))
            assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(dummyLabels.toTypedArray(), mapOf("text" to "token-2-index-0-batch-1", "type" to "Dummy")), "ENTITY")))

        }
    }

    @Test
    fun `should extract key phrases`() {
        neo4j.executeTransactionally("""CREATE (a:Article4 {body:${'$'}body, id: 1})""", mapOf("body" to "dummyText"))
        neo4j.executeTransactionally("""CREATE (a:Article4 {body:${'$'}body, id: 2})""", mapOf("body" to "dummyText"))

        neo4j.executeTransactionally("""
                    MATCH (a:Article4) WITH a ORDER BY a.id
                    WITH collect(a) AS articles
                    CALL apoc.nlp.aws.keyPhrases.stream(articles, {
                      key: ${'$'}apiKey,
                      secret: ${'$'}apiSecret,
                      nodeProperty: "body",
                      unsupportedDummyClient: true
                    })
                    YIELD value
                    RETURN value
                """.trimIndent(), mapOf("apiKey" to apiKey, "apiSecret" to apiSecret)) {
            val row1 = it.next()
            val value = row1["value"] as Map<*, *>
            val entities = value["keyPhrases"] as List<*>

            assertThat(entities, hasItem(mapOf("beginOffset" to null, "endOffset" to null, "score" to null, "text" to "keyPhrase-1-index-0-batch-0")))
            assertThat(entities, hasItem(mapOf("beginOffset" to null, "endOffset" to null, "score" to null, "text" to "keyPhrase-2-index-0-batch-0")))

            assertTrue(it.hasNext())

            val row2 = it.next()
            val value2 = row2["value"] as Map<*, *>
            val entities2 = value2["keyPhrases"] as List<*>

            assertThat(entities2, hasItem(mapOf("beginOffset" to null, "endOffset" to null, "score" to null, "text" to "keyPhrase-1-index-1-batch-0")))
            assertThat(entities2, hasItem(mapOf("beginOffset" to null, "endOffset" to null, "score" to null, "text" to "keyPhrase-2-index-1-batch-0")))
        }
    }

    @Test
    fun `should extract key phrases in batches`() {
        neo4j.executeTransactionally("""CREATE (a:Article2 {body:${'$'}body, id: 1})""", mapOf("body" to "dummyText"))

        neo4j.executeTransactionally("""
                    UNWIND range(1, 26) AS index
                    MATCH (a:Article2) WITH a ORDER BY a.id
                    WITH collect(a) AS articles
                    CALL apoc.nlp.aws.keyPhrases.stream(articles, {
                      key: ${'$'}apiKey,
                      secret: ${'$'}apiSecret,
                      nodeProperty: "body",
                      unsupportedDummyClient: true
                    })
                    YIELD value
                    RETURN collect(value) AS values
                """.trimIndent(), mapOf("apiKey" to apiKey, "apiSecret" to apiSecret)) {
            val row = it.next()
            val value = row["values"] as List<*>

            val allEntities = value.stream().flatMap { v -> ((v as Map<*, *>)["keyPhrases"] as List<*>).stream() }.collect(Collectors.toList())

            // assert that we have entries from the 2nd batch
            assertThat(allEntities, hasItem(mapOf("beginOffset" to null, "endOffset" to null, "score" to null, "text" to "keyPhrase-1-index-0-batch-1")))
            assertThat(allEntities, hasItem(mapOf("beginOffset" to null, "endOffset" to null, "score" to null, "text" to "keyPhrase-2-index-0-batch-1")))
        }
    }

    @Test
    fun `batches should create multiple key phrases virtual graphs`() {
        neo4j.executeTransactionally("""CREATE (a:Article5 {id: 1234, body:${'$'}body})""", mapOf("body" to "test"))

        var sourceNode: Node? = null
        var virtualSourceNode: Node? = null
        neo4j.executeTransactionally("MATCH (a:Article5) RETURN a", emptyMap()) {
            sourceNode = it.next()["a"] as Node
            virtualSourceNode = VirtualNode(sourceNode, sourceNode!!.propertyKeys.toList())
        }

        neo4j.executeTransactionally("""
                    UNWIND range(1, 26) AS index
                    MATCH (a:Article5) WITH a ORDER BY a.id
                    WITH collect(a) AS articles
                    CALL apoc.nlp.aws.keyPhrases.graph(articles, {
                      key: ${'$'}apiKey,
                      secret: ${'$'}apiSecret,
                      nodeProperty: "body",
                      unsupportedDummyClient: true
                    })
                    YIELD graph AS g
                    RETURN g.nodes AS nodes, g.relationships AS relationships
                """.trimIndent(), mapOf("apiKey" to apiKey, "apiSecret" to apiSecret)) {

            assertTrue(it.hasNext())
            val row1 = it.next()
            Assert.assertEquals(51, (row1["nodes"] as List<Node>).size) // 50 dummy nodes + source node

            assertTrue(it.hasNext())
            val row2 = it.next()

            val nodes: List<Node> = row2["nodes"] as List<Node>
            val relationships = row2["relationships"] as List<Relationship>
            Assert.assertEquals(3, nodes.size) // 2 dummy nodes + source node

            val dummyLabels = listOf(Label {"KeyPhrase"})

            assertThat(nodes, hasItem(sourceNode))
            assertThat(nodes, hasItem(NodeMatcher(dummyLabels, mapOf("text" to "keyPhrase-1-index-0-batch-1"))))
            assertThat(nodes, hasItem(NodeMatcher(dummyLabels, mapOf("text" to "keyPhrase-2-index-0-batch-1"))))

            Assert.assertEquals(2, relationships.size)
            assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(dummyLabels.toTypedArray(), mapOf("text" to "keyPhrase-1-index-0-batch-1")), "KEY_PHRASE")))
            assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(dummyLabels.toTypedArray(), mapOf("text" to "keyPhrase-2-index-0-batch-1")), "KEY_PHRASE")))

        }
    }

}

