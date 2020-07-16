package apoc.nlp.azure

import apoc.nlp.NodeMatcher
import apoc.nlp.RelationshipMatcher
import apoc.result.VirtualNode
import junit.framework.Assert.assertEquals
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.hasItem
import org.junit.Test
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType

class AzureVirtualKeyPhrasesGraphTest {
    @Test
    fun `create virtual graph from result with one key phrase`() {
        val sourceNode = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))

        val res = listOf(
                mapOf("id" to sourceNode.id.toString(), "keyPhrases" to listOf("foo")
        ))

        val virtualGraph = AzureVirtualKeyPhrasesGraph(res, listOf(sourceNode), RelationshipType { "KEY_PHRASE" }).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(2, nodes.size)
        assertThat(nodes, hasItem(sourceNode))

        val barLabels = listOf( Label { "KeyPhrase" })
        val barProperties = mapOf("text" to "foo")
        assertThat(nodes, hasItem(NodeMatcher(barLabels, barProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(1, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode, VirtualNode(barLabels.toTypedArray(), barProperties), "KEY_PHRASE")))
    }

    @Test
    fun `create virtual graph from result with multiple key phrases`() {
        val sourceNode = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))

        val res = listOf(
                mapOf("id" to sourceNode.id.toString(), "keyPhrases" to listOf("The Matrix", "The Notebook"))
        )

        val virtualGraph = AzureVirtualKeyPhrasesGraph(res, listOf(sourceNode), RelationshipType { "KEY_PHRASE" }).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(3, nodes.size)
        assertThat(nodes, hasItem(sourceNode))

        val matrixNode = VirtualNode(arrayOf(Label{"KeyPhrase"}), mapOf("text" to "The Matrix"))
        val notebookNode = VirtualNode(arrayOf(Label{"KeyPhrase"}), mapOf("text" to "The Notebook"))

        assertThat(nodes, hasItem(NodeMatcher(matrixNode.labels.toList(), matrixNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(notebookNode.labels.toList(), notebookNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(2, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode, matrixNode, "KEY_PHRASE")))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode, notebookNode, "KEY_PHRASE")))
    }

    @Test
    fun `create virtual graph from result with duplicate entities`() {
        val sourceNode = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))

        val res = listOf(
                mapOf("id" to sourceNode.id.toString(), "keyPhrases" to listOf("The Matrix", "The Matrix"))
        )

        val virtualGraph = AzureVirtualKeyPhrasesGraph(res, listOf(sourceNode), RelationshipType { "KEY_PHRASE" }).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(2, nodes.size)
        assertThat(nodes, hasItem(sourceNode))

        val matrixNode = VirtualNode(arrayOf(Label{"KeyPhrase"}), mapOf("text" to "The Matrix"))

        assertThat(nodes, hasItem(NodeMatcher(matrixNode.labels.toList(), matrixNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(1, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode, matrixNode, "KEY_PHRASE")))
    }

    @Test
    fun `create virtual graph from result with multiple source nodes`() {
        val sourceNode1 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))
        val sourceNode2 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 5678L))

        val res = listOf(
                mapOf("id" to sourceNode1.id.toString(), "keyPhrases" to listOf("The Matrix", "The Notebook")),
                mapOf("id" to sourceNode2.id.toString(), "keyPhrases" to listOf("Toy Story", "Titanic"))
        )

        val virtualGraph = AzureVirtualKeyPhrasesGraph(res, listOf(sourceNode1, sourceNode2), RelationshipType { "KEY_PHRASE" }).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(6, nodes.size)
        assertThat(nodes, hasItem(sourceNode1))
        assertThat(nodes, hasItem(sourceNode2))

        val matrixNode = VirtualNode(arrayOf( Label{"KeyPhrase"}), mapOf("text" to "The Matrix"))
        val notebookNode = VirtualNode(arrayOf(Label{"KeyPhrase"}), mapOf("text" to "The Notebook"))
        val toyStoryNode = VirtualNode(arrayOf(Label{"KeyPhrase"}), mapOf("text" to "Toy Story"))
        val titanicNode = VirtualNode(arrayOf( Label{"KeyPhrase"}), mapOf("text" to "Titanic"))

        assertThat(nodes, hasItem(NodeMatcher(matrixNode.labels.toList(), matrixNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(notebookNode.labels.toList(), notebookNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(toyStoryNode.labels.toList(), toyStoryNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(titanicNode.labels.toList(), titanicNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(4, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, matrixNode, "KEY_PHRASE" )))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, notebookNode, "KEY_PHRASE")))

        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, toyStoryNode, "KEY_PHRASE")))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, titanicNode, "KEY_PHRASE")))
    }

    @Test
    fun `create virtual graph from result with multiple source nodes with overlapping entities`() {
        val sourceNode1 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))
        val sourceNode2 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 5678L))

        val res = listOf(
                mapOf("id" to sourceNode1.id.toString(), "keyPhrases" to listOf("The Matrix", "The Notebook")),
                mapOf("id" to sourceNode2.id.toString(), "keyPhrases" to listOf("Titanic", "The Matrix", "Top Boy")))

        val virtualGraph = AzureVirtualKeyPhrasesGraph(res, listOf(sourceNode1, sourceNode2), RelationshipType { "KEY_PHRASE" }).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(6, nodes.size)
        assertThat(nodes, hasItem(sourceNode1))
        assertThat(nodes, hasItem(sourceNode2))

        val matrixNode = VirtualNode(arrayOf(Label{"KeyPhrase"}), mapOf("text" to "The Matrix"))
        val notebookNode = VirtualNode(arrayOf(Label{"KeyPhrase"}), mapOf("text" to "The Notebook"))
        val titanicNode = VirtualNode(arrayOf(Label{"KeyPhrase"}), mapOf("text" to "Titanic"))
        val topBoyNode = VirtualNode(arrayOf( Label{"KeyPhrase"}), mapOf("text" to "Top Boy"))

        assertThat(nodes, hasItem(NodeMatcher(matrixNode.labels.toList(), matrixNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(notebookNode.labels.toList(), notebookNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(titanicNode.labels.toList(), titanicNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(topBoyNode.labels.toList(), topBoyNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(5, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, matrixNode, "KEY_PHRASE")))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, notebookNode, "KEY_PHRASE")))

        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, titanicNode, "KEY_PHRASE")))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, matrixNode, "KEY_PHRASE")))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, topBoyNode, "KEY_PHRASE")))
    }

}

