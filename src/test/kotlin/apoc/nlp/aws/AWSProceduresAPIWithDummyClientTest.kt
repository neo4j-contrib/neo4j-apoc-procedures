package apoc.nlp.aws

import apoc.nlp.NodeMatcher
import apoc.nlp.RelationshipMatcher
import apoc.result.VirtualNode
import apoc.util.TestUtil
import org.junit.Assert
import org.junit.Assert.assertTrue
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers
import org.hamcrest.Matchers.hasItem
import org.hamcrest.collection.IsMapContaining
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

            assertThat(entities, hasItem(mapOf("beginOffset" to null, "endOffset" to null, "score" to 0.5F, "text" to "token-1-index-0-batch-0", "type" to "COMMERCIAL_ITEM")))
            assertThat(entities, hasItem(mapOf("beginOffset" to null, "endOffset" to null, "score" to 0.7F, "text" to "token-2-index-0-batch-0", "type" to "ORGANIZATION")))

            assertTrue(it.hasNext())

            val row2 = it.next()
            val value2 = row2["value"] as Map<*, *>
            val entities2 = value2["entities"] as List<*>

            assertThat(entities2, hasItem(mapOf("beginOffset" to null, "endOffset" to null, "score" to 0.5F, "text" to "token-1-index-1-batch-0", "type" to "COMMERCIAL_ITEM")))
            assertThat(entities2, hasItem(mapOf("beginOffset" to null, "endOffset" to null, "score" to 0.7F, "text" to "token-2-index-1-batch-0", "type" to "ORGANIZATION")))
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
            assertThat(allEntities, hasItem(mapOf("beginOffset" to null, "endOffset" to null, "score" to 0.5F, "text" to "token-1-index-0-batch-1", "type" to "COMMERCIAL_ITEM")))
            assertThat(allEntities, hasItem(mapOf("beginOffset" to null, "endOffset" to null, "score" to 0.7F, "text" to "token-2-index-0-batch-1", "type" to "ORGANIZATION")))
        }
    }

    @Test
    fun `create virtual entity graph based on score cut off`() {
        neo4j.executeTransactionally("""CREATE (a:Article10 {id: 1234, body:${'$'}body})""", mapOf("body" to "test"))

        var sourceNode: Node? = null
        var virtualSourceNode: Node? = null
        neo4j.executeTransactionally("MATCH (a:Article10) RETURN a", emptyMap()) {
            sourceNode = it.next()["a"] as Node
            virtualSourceNode = VirtualNode(sourceNode, sourceNode!!.propertyKeys.toList())
        }

        neo4j.executeTransactionally("""
                    MATCH (a:Article10) WITH a ORDER BY a.id
                    WITH collect(a) AS articles
                    CALL apoc.nlp.aws.entities.graph(articles, {
                      key: ${'$'}apiKey,
                      secret: ${'$'}apiSecret,
                      nodeProperty: "body",
                      unsupportedDummyClient: true,
                      scoreCutoff: 0.6,
                      writeRelationshipType: "HAS_ENTITY",
                      writeRelationshipProperty: "myScore"
                    })
                    YIELD graph AS g
                    RETURN g.nodes AS nodes, g.relationships AS relationships
                """.trimIndent(), mapOf("apiKey" to apiKey, "apiSecret" to apiSecret)) {
            assertTrue(it.hasNext())
            val row2 = it.next()

            val nodes: List<Node> = row2["nodes"] as List<Node>
            val relationships = row2["relationships"] as List<Relationship>
            Assert.assertEquals(2, nodes.size) // 2 dummy nodes + source node

            val dummyLabels2 = listOf(Label { "Organization"}, Label {"Entity"})

            assertThat(nodes, hasItem(sourceNode))
            assertThat(nodes, hasItem(NodeMatcher(dummyLabels2, mapOf("text" to "token-2-index-0-batch-0", "type" to "ORGANIZATION"))))

            Assert.assertEquals(1, relationships.size)
            assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(dummyLabels2.toTypedArray(), mapOf("text" to "token-2-index-0-batch-0", "type" to "ORGANIZATION")), "HAS_ENTITY", mapOf("myScore" to 0.7F))))
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
                    UNWIND range(1,26) AS id
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

            val dummyLabels1 = listOf(Label { "CommercialItem"}, Label {"Entity"})
            val dummyLabels2 = listOf(Label { "Organization"}, Label {"Entity"})

            assertThat(nodes, hasItem(sourceNode))
            assertThat(nodes, hasItem(NodeMatcher(dummyLabels1, mapOf("text" to "token-1-index-0-batch-1", "type" to "COMMERCIAL_ITEM"))))
            assertThat(nodes, hasItem(NodeMatcher(dummyLabels2, mapOf("text" to "token-2-index-0-batch-1", "type" to "ORGANIZATION"))))

            Assert.assertEquals(2, relationships.size)
            assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(dummyLabels1.toTypedArray(), mapOf("text" to "token-1-index-0-batch-1", "type" to "COMMERCIAL_ITEM")), "ENTITY", mapOf("score" to 0.5F))))
            assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(dummyLabels2.toTypedArray(), mapOf("text" to "token-2-index-0-batch-1", "type" to "ORGANIZATION")), "ENTITY", mapOf("score" to 0.7F))))
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

            assertThat(entities, hasItem(mapOf("beginOffset" to null, "endOffset" to null, "score" to 0.3F, "text" to "keyPhrase-1-index-0-batch-0")))
            assertThat(entities, hasItem(mapOf("beginOffset" to null, "endOffset" to null, "score" to 0.4F, "text" to "keyPhrase-2-index-0-batch-0")))

            assertTrue(it.hasNext())

            val row2 = it.next()
            val value2 = row2["value"] as Map<*, *>
            val entities2 = value2["keyPhrases"] as List<*>

            assertThat(entities2, hasItem(mapOf("beginOffset" to null, "endOffset" to null, "score" to 0.3F, "text" to "keyPhrase-1-index-1-batch-0")))
            assertThat(entities2, hasItem(mapOf("beginOffset" to null, "endOffset" to null, "score" to 0.4F, "text" to "keyPhrase-2-index-1-batch-0")))
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
            assertThat(allEntities, hasItem(mapOf("beginOffset" to null, "endOffset" to null, "score" to 0.3F, "text" to "keyPhrase-1-index-0-batch-1")))
            assertThat(allEntities, hasItem(mapOf("beginOffset" to null, "endOffset" to null, "score" to 0.4F, "text" to "keyPhrase-2-index-0-batch-1")))
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
            assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(dummyLabels.toTypedArray(), mapOf("text" to "keyPhrase-1-index-0-batch-1")), "KEY_PHRASE", mapOf("score" to 0.3F))))
            assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(dummyLabels.toTypedArray(), mapOf("text" to "keyPhrase-2-index-0-batch-1")), "KEY_PHRASE", mapOf("score" to 0.4F))))
        }
    }

    @Test
    fun `create virtual key phrase graph based on score cut off`() {
        neo4j.executeTransactionally("""CREATE (a:Article11 {id: 1234, body:${'$'}body})""", mapOf("body" to "test"))

        var sourceNode: Node? = null
        var virtualSourceNode: Node? = null
        neo4j.executeTransactionally("MATCH (a:Article11) RETURN a", emptyMap()) {
            sourceNode = it.next()["a"] as Node
            virtualSourceNode = VirtualNode(sourceNode, sourceNode!!.propertyKeys.toList())
        }

        neo4j.executeTransactionally(""" 
                    MATCH (a:Article11) WITH a ORDER BY a.id
                    WITH collect(a) AS articles
                    CALL apoc.nlp.aws.keyPhrases.graph(articles, {
                      key: ${'$'}apiKey,
                      secret: ${'$'}apiSecret,
                      nodeProperty: "body",
                      unsupportedDummyClient: true,
                      scoreCutoff: 0.35,
                      writeRelationshipType: "HAS_KEY_PHRASE",
                      writeRelationshipProperty: "myScore"
                    })
                    YIELD graph AS g
                    RETURN g.nodes AS nodes, g.relationships AS relationships
                """.trimIndent(), mapOf("apiKey" to apiKey, "apiSecret" to apiSecret)) {

            assertTrue(it.hasNext())
            val row = it.next()

            val nodes: List<Node> = row["nodes"] as List<Node>
            val relationships = row["relationships"] as List<Relationship>
            Assert.assertEquals(2, nodes.size) // 1 dummy nodes + source node

            val dummyLabels = listOf(Label {"KeyPhrase"})

            assertThat(nodes, hasItem(sourceNode))
            assertThat(nodes, hasItem(NodeMatcher(dummyLabels, mapOf("text" to "keyPhrase-2-index-0-batch-0"))))

            Assert.assertEquals(1, relationships.size)
            assertThat(relationships, hasItem(RelationshipMatcher(virtualSourceNode, VirtualNode(dummyLabels.toTypedArray(), mapOf("text" to "keyPhrase-2-index-0-batch-0")), "HAS_KEY_PHRASE", mapOf("myScore" to 0.4F))))
        }
    }

    @Test
    fun `should extract sentiment`() {
        neo4j.executeTransactionally("""CREATE (a:Article6 {body:${'$'}body, id: 1})""", mapOf("body" to "dummyText"))
        neo4j.executeTransactionally("""CREATE (a:Article6 {body:${'$'}body, id: 2})""", mapOf("body" to "dummyText"))

        neo4j.executeTransactionally("""
                    MATCH (a:Article6) WITH a ORDER BY a.id
                    WITH collect(a) AS articles
                    CALL apoc.nlp.aws.sentiment.stream(articles, {
                      key: ${'$'}apiKey,
                      secret: ${'$'}apiSecret,
                      nodeProperty: "body",
                      unsupportedDummyClient: true
                    })
                    YIELD value
                    RETURN value
                """.trimIndent(), mapOf("apiKey" to apiKey, "apiSecret" to apiSecret)) {
            val row1 = it.next()
            val value= row1["value"] as Map<String, Any>

            assertThat(value, IsMapContaining.hasEntry("sentiment", "MIXED" as Any))
            assertThat(value["sentimentScore"] as Map<String, Any>, Matchers.`is`(mapOf("negative" to null, "neutral" to null, "mixed" to 0.7F, "positive" to null) as Map<String, Any>))

            assertTrue(it.hasNext())

            val row2 = it.next()
            val value2 = row2["value"] as Map<String, Any>

            assertThat(value2, IsMapContaining.hasEntry("sentiment", "MIXED" as Any))
            assertThat(value2["sentimentScore"] as Map<String, Any>, Matchers.`is`(mapOf("negative" to null, "neutral" to null, "mixed" to 0.7F, "positive" to null)  as Map<String, Any>))
        }
    }

    @Test
    fun `batches should create multiple sentiment virtual graphs`() {
        neo4j.executeTransactionally("""CREATE (a:Article7 {id: 1234, body:${'$'}body})""", mapOf("body" to "test"))

        var sourceNode: Node? = null
        var virtualSourceNode: Node? = null
        neo4j.executeTransactionally("MATCH (a:Article7) RETURN a", emptyMap()) {
            sourceNode = it.next()["a"] as Node
            virtualSourceNode = VirtualNode(sourceNode, sourceNode!!.propertyKeys.toList())
        }

        neo4j.executeTransactionally("""
                    UNWIND range(1, 26) AS index
                    MATCH (a:Article7) WITH a ORDER BY a.id
                    WITH collect(a) AS articles
                    CALL apoc.nlp.aws.sentiment.graph(articles, {
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
            Assert.assertEquals(1, (row1["nodes"] as List<Node>).size) // source node

            assertTrue(it.hasNext())
            val row2 = it.next()

            val nodes: List<Node> = row2["nodes"] as List<Node>
            Assert.assertEquals(1, nodes.size) // source node

            assertThat(nodes, hasItem(sourceNode))
            assertThat(nodes, hasItem(NodeMatcher(listOf(Label { "Article7" }), mapOf("id" to 1234L, "body" to "test", "sentiment" to "Mixed", "sentimentScore" to 0.7F))))
        }
    }

}

