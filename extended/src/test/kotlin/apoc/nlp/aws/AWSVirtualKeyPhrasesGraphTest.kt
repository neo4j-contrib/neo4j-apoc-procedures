package apoc.nlp.aws

import apoc.nlp.NodeMatcher
import apoc.nlp.RelationshipMatcher
import apoc.result.VirtualNode
import com.amazonaws.services.comprehend.model.*
import org.junit.Assert.assertEquals
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.hasItem
import org.junit.Test
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType

class AWSVirtualKeyPhrasesGraphTest {
    @Test
    fun `create virtual graph from result with one key phrase`() {
        val entity = KeyPhrase().withText("foo").withScore(0.3F)
        val itemResult = BatchDetectKeyPhrasesItemResult().withKeyPhrases(entity).withIndex(0)
        val res = BatchDetectKeyPhrasesResult().withErrorList(BatchItemError()).withResultList(itemResult)
        val sourceNode = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))

        val virtualGraph = AWSVirtualKeyPhrasesGraph(res, listOf(sourceNode), RelationshipType { "KEY_PHRASE" }, "score", 0.0).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(2, nodes.size)
        assertThat(nodes, hasItem(sourceNode))

        val barLabels = listOf(Label { "KeyPhrase" })
        val barProperties = mapOf("text" to "foo")
        assertThat(nodes, hasItem(NodeMatcher(barLabels, barProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(1, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode, VirtualNode(barLabels.toTypedArray(), barProperties), "KEY_PHRASE", mapOf("score" to 0.3F))))
    }

    @Test
    fun `create virtual graph from result with multiple entities`() {
        val result = BatchDetectKeyPhrasesItemResult().withKeyPhrases(
                KeyPhrase().withText("The Matrix").withScore(0.4F),
                KeyPhrase().withText("The Notebook").withScore(0.6F))
                .withIndex(0)

        val res = BatchDetectKeyPhrasesResult().withErrorList(BatchItemError()).withResultList(result)
        val sourceNode = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))

        val virtualGraph = AWSVirtualKeyPhrasesGraph(res, listOf(sourceNode), RelationshipType { "KEY_PHRASE" }, "score", 0.0).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(3, nodes.size)
        assertThat(nodes, hasItem(sourceNode))

        val matrixNode = VirtualNode(arrayOf(Label{"KeyPhrase"}), mapOf("text" to "The Matrix"))
        val notebookNode = VirtualNode(arrayOf( Label{"KeyPhrase"}), mapOf("text" to "The Notebook"))

        assertThat(nodes, hasItem(NodeMatcher(matrixNode.labels.toList(), matrixNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(notebookNode.labels.toList(), notebookNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(2, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode, matrixNode, "KEY_PHRASE", mapOf("score" to 0.4F))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode, notebookNode, "KEY_PHRASE", mapOf("score" to 0.6F))))
    }

    @Test
    fun `create virtual graph from result with duplicate entities`() {
        val result = BatchDetectKeyPhrasesItemResult().withKeyPhrases(
                KeyPhrase().withText("The Matrix").withScore(0.4F),
                KeyPhrase().withText("The Matrix").withScore(0.6F))
                .withIndex(0)

        val res = BatchDetectKeyPhrasesResult().withErrorList(BatchItemError()).withResultList(result)
        val sourceNode = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))

        val virtualGraph = AWSVirtualKeyPhrasesGraph(res, listOf(sourceNode), RelationshipType { "KEY_PHRASE" }, "score", 0.0).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(2, nodes.size)
        assertThat(nodes, hasItem(sourceNode))

        val matrixNode = VirtualNode(arrayOf(Label{"KeyPhrase"}), mapOf("text" to "The Matrix"))

        assertThat(nodes, hasItem(NodeMatcher(matrixNode.labels.toList(), matrixNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(1, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode, matrixNode, "KEY_PHRASE", mapOf("score" to 0.6F))))
    }

    @Test
    fun `create virtual graph from result with multiple source nodes`() {
        val itemResult1 = BatchDetectKeyPhrasesItemResult().withKeyPhrases(
                KeyPhrase().withText("The Matrix").withScore(0.75F),
                KeyPhrase().withText("The Notebook").withScore(0.8F))
                .withIndex(0)

        val itemResult2 = BatchDetectKeyPhrasesItemResult().withKeyPhrases(
                KeyPhrase().withText("Toy Story").withScore(0.85F),
                KeyPhrase().withText("Titanic").withScore(0.95F))
                .withIndex(1)

        val res = BatchDetectKeyPhrasesResult().withErrorList(BatchItemError()).withResultList(itemResult1, itemResult2)
        val sourceNode1 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))
        val sourceNode2 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 5678L))

        val virtualGraph = AWSVirtualKeyPhrasesGraph(res, listOf(sourceNode1, sourceNode2), RelationshipType { "KEY_PHRASE" }, "score", 0.0).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(6, nodes.size)
        assertThat(nodes, hasItem(sourceNode1))
        assertThat(nodes, hasItem(sourceNode2))

        val matrixNode = VirtualNode(arrayOf(Label{"KeyPhrase"}), mapOf("text" to "The Matrix"))
        val notebookNode = VirtualNode(arrayOf(Label{"KeyPhrase"}), mapOf("text" to "The Notebook"))
        val toyStoryNode = VirtualNode(arrayOf(Label{"KeyPhrase"}), mapOf("text" to "Toy Story"))
        val titanicNode = VirtualNode(arrayOf(Label{"KeyPhrase"}), mapOf("text" to "Titanic"))

        assertThat(nodes, hasItem(NodeMatcher(matrixNode.labels.toList(), matrixNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(notebookNode.labels.toList(), notebookNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(toyStoryNode.labels.toList(), toyStoryNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(titanicNode.labels.toList(), titanicNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(4, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, matrixNode, "KEY_PHRASE", mapOf("score" to 0.75F))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, notebookNode, "KEY_PHRASE", mapOf("score" to 0.8F))))

        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, toyStoryNode, "KEY_PHRASE", mapOf("score" to 0.85F))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, titanicNode, "KEY_PHRASE", mapOf("score" to 0.95F))))
    }

    @Test
    fun `create virtual graph from result with multiple source nodes with overlapping entities`() {
        val itemResult1 = BatchDetectKeyPhrasesItemResult().withKeyPhrases(
                KeyPhrase().withText("The Matrix").withScore(0.15F),
                KeyPhrase().withText("The Notebook").withScore(0.25F))
                .withIndex(0)

        val itemResult2 = BatchDetectKeyPhrasesItemResult().withKeyPhrases(
                KeyPhrase().withText("The Matrix").withScore(0.35F),
                KeyPhrase().withText("Titanic").withScore(0.45F),
                KeyPhrase().withText("Top Boy").withScore(0.55F))
                .withIndex(1)

        val res = BatchDetectKeyPhrasesResult().withErrorList(BatchItemError()).withResultList(itemResult1, itemResult2)
        val sourceNode1 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))
        val sourceNode2 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 5678L))

        val virtualGraph = AWSVirtualKeyPhrasesGraph(res, listOf(sourceNode1, sourceNode2), RelationshipType { "KEY_PHRASE" }, "score", 0.0).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(6, nodes.size)
        assertThat(nodes, hasItem(sourceNode1))
        assertThat(nodes, hasItem(sourceNode2))

        val matrixNode = VirtualNode(arrayOf(Label{"KeyPhrase"}), mapOf("text" to "The Matrix"))
        val notebookNode = VirtualNode(arrayOf(Label{"KeyPhrase"}), mapOf("text" to "The Notebook"))
        val titanicNode = VirtualNode(arrayOf(Label{"KeyPhrase"}), mapOf("text" to "Titanic"))
        val topBoyNode = VirtualNode(arrayOf(Label{"KeyPhrase"}), mapOf("text" to "Top Boy"))

        assertThat(nodes, hasItem(NodeMatcher(matrixNode.labels.toList(), matrixNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(notebookNode.labels.toList(), notebookNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(titanicNode.labels.toList(), titanicNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(topBoyNode.labels.toList(), topBoyNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(5, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, matrixNode, "KEY_PHRASE", mapOf("score" to 0.15F))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, notebookNode, "KEY_PHRASE", mapOf("score" to 0.25F))))

        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, matrixNode, "KEY_PHRASE", mapOf("score" to 0.35F))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, titanicNode, "KEY_PHRASE", mapOf("score" to 0.45F))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, topBoyNode, "KEY_PHRASE", mapOf("score" to 0.55F))))
    }

    @Test
    fun `create graph based on confidence cut off`() {
        val itemResult1 = BatchDetectKeyPhrasesItemResult().withKeyPhrases(
                KeyPhrase().withText("The Matrix").withScore(0.15F),
                KeyPhrase().withText("The Notebook").withScore(0.25F))
                .withIndex(0)

        val itemResult2 = BatchDetectKeyPhrasesItemResult().withKeyPhrases(
                KeyPhrase().withText("The Matrix").withScore(0.35F),
                KeyPhrase().withText("Titanic").withScore(0.45F),
                KeyPhrase().withText("Top Boy").withScore(0.55F))
                .withIndex(1)

        val res = BatchDetectKeyPhrasesResult().withErrorList(BatchItemError()).withResultList(itemResult1, itemResult2)
        val sourceNode1 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))
        val sourceNode2 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 5678L))

        val virtualGraph = AWSVirtualKeyPhrasesGraph(res, listOf(sourceNode1, sourceNode2), RelationshipType { "KEY_PHRASE" }, "score", 0.2).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(6, nodes.size)
        assertThat(nodes, hasItem(sourceNode1))
        assertThat(nodes, hasItem(sourceNode2))

        val matrixNode = VirtualNode(arrayOf(Label{"KeyPhrase"}), mapOf("text" to "The Matrix"))
        val notebookNode = VirtualNode(arrayOf(Label{"KeyPhrase"}), mapOf("text" to "The Notebook"))
        val titanicNode = VirtualNode(arrayOf(Label{"KeyPhrase"}), mapOf("text" to "Titanic"))
        val topBoyNode = VirtualNode(arrayOf(Label{"KeyPhrase"}), mapOf("text" to "Top Boy"))

        assertThat(nodes, hasItem(NodeMatcher(matrixNode.labels.toList(), matrixNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(notebookNode.labels.toList(), notebookNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(titanicNode.labels.toList(), titanicNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(topBoyNode.labels.toList(), topBoyNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(4, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, notebookNode, "KEY_PHRASE", mapOf("score" to 0.25F))))

        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, matrixNode, "KEY_PHRASE", mapOf("score" to 0.35F))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, titanicNode, "KEY_PHRASE", mapOf("score" to 0.45F))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, topBoyNode, "KEY_PHRASE", mapOf("score" to 0.55F))))
    }

}

