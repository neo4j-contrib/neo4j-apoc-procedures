package apoc.nlp.aws

import apoc.nlp.AWSVirtualEntitiesGraph
import apoc.nlp.AWSVirtualEntitiesGraph.Companion.ENTITY_MAPPING
import apoc.nlp.AWSVirtualKeyPhrasesGraph
import apoc.result.VirtualNode
import com.amazonaws.services.comprehend.model.*
import junit.framework.Assert.assertEquals
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.hasItem
import org.junit.Test
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType

class AWSVirtualKeyPhrasesGraphTest {
    @Test
    fun `create virtual graph from result with one key phrase`() {
        val entity = KeyPhrase().withText("foo")
        val itemResult = BatchDetectKeyPhrasesItemResult().withKeyPhrases(entity).withIndex(0)
        val res = BatchDetectKeyPhrasesResult().withErrorList(BatchItemError()).withResultList(itemResult)
        val sourceNode = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))

        val virtualGraph = AWSVirtualKeyPhrasesGraph(res, listOf(sourceNode), RelationshipType { "KEY_PHRASE" }).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(2, nodes.size)
        assertThat(nodes, hasItem(sourceNode))

        val barLabels = listOf(Label { "Keyphrase" })
        val barProperties = mapOf("text" to "foo")
        assertThat(nodes, hasItem(NodeMatcher(barLabels, barProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(1, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode, VirtualNode(barLabels.toTypedArray(), barProperties), "KEY_PHRASE")))
    }

    @Test
    fun `create virtual graph from result with multiple entities`() {
        val result = BatchDetectKeyPhrasesItemResult().withKeyPhrases(
                KeyPhrase().withText("The Matrix"),
                KeyPhrase().withText("The Notebook"))
                .withIndex(0)

        val res = BatchDetectKeyPhrasesResult().withErrorList(BatchItemError()).withResultList(result)
        val sourceNode = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))

        val virtualGraph = AWSVirtualKeyPhrasesGraph(res, listOf(sourceNode), RelationshipType { "KEY_PHRASE" }).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(3, nodes.size)
        assertThat(nodes, hasItem(sourceNode))

        val matrixNode = VirtualNode(arrayOf(Label{"Keyphrase"}), mapOf("text" to "The Matrix"))
        val notebookNode = VirtualNode(arrayOf( Label{"Keyphrase"}), mapOf("text" to "The Notebook"))

        assertThat(nodes, hasItem(NodeMatcher(matrixNode.labels.toList(), matrixNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(notebookNode.labels.toList(), notebookNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(2, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode, matrixNode, "KEY_PHRASE")))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode, notebookNode, "KEY_PHRASE")))
    }

    @Test
    fun `create virtual graph from result with multiple source nodes`() {
        val itemResult1 = BatchDetectKeyPhrasesItemResult().withKeyPhrases(
                KeyPhrase().withText("The Matrix"),
                KeyPhrase().withText("The Notebook"))
                .withIndex(0)

        val itemResult2 = BatchDetectKeyPhrasesItemResult().withKeyPhrases(
                KeyPhrase().withText("Toy Story"),
                KeyPhrase().withText("Titanic"))
                .withIndex(1)

        val res = BatchDetectKeyPhrasesResult().withErrorList(BatchItemError()).withResultList(itemResult1, itemResult2)
        val sourceNode1 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))
        val sourceNode2 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 5678L))

        val virtualGraph = AWSVirtualKeyPhrasesGraph(res, listOf(sourceNode1, sourceNode2), RelationshipType { "KEY_PHRASE" }).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(6, nodes.size)
        assertThat(nodes, hasItem(sourceNode1))
        assertThat(nodes, hasItem(sourceNode2))

        val matrixNode = VirtualNode(arrayOf(Label{"Keyphrase"}), mapOf("text" to "The Matrix"))
        val notebookNode = VirtualNode(arrayOf(Label{"Keyphrase"}), mapOf("text" to "The Notebook"))
        val toyStoryNode = VirtualNode(arrayOf(Label{"Keyphrase"}), mapOf("text" to "Toy Story"))
        val titanicNode = VirtualNode(arrayOf(Label{"Keyphrase"}), mapOf("text" to "Titanic"))

        assertThat(nodes, hasItem(NodeMatcher(matrixNode.labels.toList(), matrixNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(notebookNode.labels.toList(), notebookNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(toyStoryNode.labels.toList(), toyStoryNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(titanicNode.labels.toList(), titanicNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(4, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, matrixNode, "KEY_PHRASE")))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, notebookNode, "KEY_PHRASE")))

        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, toyStoryNode, "KEY_PHRASE")))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, titanicNode, "KEY_PHRASE")))
    }

    @Test
    fun `create virtual graph from result with multiple source nodes with overlapping entities`() {
        val itemResult1 = BatchDetectKeyPhrasesItemResult().withKeyPhrases(
                KeyPhrase().withText("The Matrix"),
                KeyPhrase().withText("The Notebook"))
                .withIndex(0)

        val itemResult2 = BatchDetectKeyPhrasesItemResult().withKeyPhrases(
                KeyPhrase().withText("The Matrix"),
                KeyPhrase().withText("Titanic"),
                KeyPhrase().withText("Top Boy"))
                .withIndex(1)

        val res = BatchDetectKeyPhrasesResult().withErrorList(BatchItemError()).withResultList(itemResult1, itemResult2)
        val sourceNode1 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))
        val sourceNode2 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 5678L))

        val virtualGraph = AWSVirtualKeyPhrasesGraph(res, listOf(sourceNode1, sourceNode2), RelationshipType { "KEY_PHRASE" }).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(6, nodes.size)
        assertThat(nodes, hasItem(sourceNode1))
        assertThat(nodes, hasItem(sourceNode2))

        val matrixNode = VirtualNode(arrayOf(Label{"Keyphrase"}), mapOf("text" to "The Matrix"))
        val notebookNode = VirtualNode(arrayOf(Label{"Keyphrase"}), mapOf("text" to "The Notebook"))
        val titanicNode = VirtualNode(arrayOf(Label{"Keyphrase"}), mapOf("text" to "Titanic"))
        val topBoyNode = VirtualNode(arrayOf(Label{"Keyphrase"}), mapOf("text" to "Top Boy"))

        assertThat(nodes, hasItem(NodeMatcher(matrixNode.labels.toList(), matrixNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(notebookNode.labels.toList(), notebookNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(titanicNode.labels.toList(), titanicNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(topBoyNode.labels.toList(), topBoyNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(5, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, matrixNode, "KEY_PHRASE")))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, notebookNode, "KEY_PHRASE")))

        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, matrixNode, "KEY_PHRASE")))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, titanicNode, "KEY_PHRASE")))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, topBoyNode, "KEY_PHRASE")))
    }

}

