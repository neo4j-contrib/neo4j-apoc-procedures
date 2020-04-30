package apoc.nlp.aws

import apoc.nlp.AWSVirtualEntitiesGraph
import apoc.nlp.AWSVirtualEntitiesGraph.Companion.ENTITY_MAPPING
import apoc.result.VirtualNode
import com.amazonaws.services.comprehend.model.BatchDetectEntitiesItemResult
import com.amazonaws.services.comprehend.model.BatchDetectEntitiesResult
import com.amazonaws.services.comprehend.model.BatchItemError
import com.amazonaws.services.comprehend.model.Entity
import junit.framework.Assert.assertEquals
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.hasItem
import org.junit.Test
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType

class AWSVirtualEntitiesGraphTest {
    @Test
    fun `create virtual graph from result with one entity`() {
        val entity = Entity().withText("foo").withType("Bar")
        val itemResult = BatchDetectEntitiesItemResult().withEntities(entity).withIndex(0)
        val res = BatchDetectEntitiesResult().withErrorList(BatchItemError()).withResultList(itemResult)
        val sourceNode = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))

        val virtualGraph = AWSVirtualEntitiesGraph(res, listOf(sourceNode), RelationshipType { "ENTITY" }).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(2, nodes.size)
        assertThat(nodes, hasItem(sourceNode))

        val barLabels = listOf(Label { "Bar" }, Label { "Entity" })
        val barProperties = mapOf("text" to "foo", "type" to "Bar")
        assertThat(nodes, hasItem(NodeMatcher(barLabels, barProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(1, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode, VirtualNode(barLabels.toTypedArray(), barProperties), "ENTITY")))
    }

    @Test
    fun `create virtual graph from result with multiple entities`() {
        val result = BatchDetectEntitiesItemResult().withEntities(
                Entity().withText("The Matrix").withType("Movie"),
                Entity().withText("The Notebook").withType("Movie"))
                .withIndex(0)

        val res = BatchDetectEntitiesResult().withErrorList(BatchItemError()).withResultList(result)
        val sourceNode = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))

        val virtualGraph = AWSVirtualEntitiesGraph(res, listOf(sourceNode), RelationshipType { "ENTITY" }).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(3, nodes.size)
        assertThat(nodes, hasItem(sourceNode))

        val matrixNode = VirtualNode(arrayOf(Label{"Movie"}, Label{"Entity"}), mapOf("text" to "The Matrix", "type" to "Movie"))
        val notebookNode = VirtualNode(arrayOf(Label{"Movie"}, Label{"Entity"}), mapOf("text" to "The Notebook", "type" to "Movie"))

        assertThat(nodes, hasItem(NodeMatcher(matrixNode.labels.toList(), matrixNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(notebookNode.labels.toList(), notebookNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(2, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode, matrixNode, "ENTITY")))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode, notebookNode, "ENTITY")))
    }

    @Test
    fun `create virtual graph from result with multiple source nodes`() {
        val itemResult1 = BatchDetectEntitiesItemResult().withEntities(
                Entity().withText("The Matrix").withType("Movie"),
                Entity().withText("The Notebook").withType("Movie"))
                .withIndex(0)

        val itemResult2 = BatchDetectEntitiesItemResult().withEntities(
                Entity().withText("Toy Story").withType("Movie"),
                Entity().withText("Titanic").withType("Movie"))
                .withIndex(1)

        val res = BatchDetectEntitiesResult().withErrorList(BatchItemError()).withResultList(itemResult1, itemResult2)
        val sourceNode1 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))
        val sourceNode2 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 5678L))

        val virtualGraph = AWSVirtualEntitiesGraph(res, listOf(sourceNode1, sourceNode2), RelationshipType { "ENTITY" }).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(6, nodes.size)
        assertThat(nodes, hasItem(sourceNode1))
        assertThat(nodes, hasItem(sourceNode2))

        val matrixNode = VirtualNode(arrayOf(Label{"Movie"}, Label{"Entity"}), mapOf("text" to "The Matrix", "type" to "Movie"))
        val notebookNode = VirtualNode(arrayOf(Label{"Movie"}, Label{"Entity"}), mapOf("text" to "The Notebook", "type" to "Movie"))
        val toyStoryNode = VirtualNode(arrayOf(Label{"Movie"}, Label{"Entity"}), mapOf("text" to "Toy Story", "type" to "Movie"))
        val titanicNode = VirtualNode(arrayOf(Label{"Movie"}, Label{"Entity"}), mapOf("text" to "Titanic", "type" to "Movie"))

        assertThat(nodes, hasItem(NodeMatcher(matrixNode.labels.toList(), matrixNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(notebookNode.labels.toList(), notebookNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(toyStoryNode.labels.toList(), toyStoryNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(titanicNode.labels.toList(), titanicNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(4, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, matrixNode, "ENTITY")))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, notebookNode, "ENTITY")))

        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, toyStoryNode, "ENTITY")))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, titanicNode, "ENTITY")))
    }

    @Test
    fun `create virtual graph from result with multiple source nodes with overlapping entities`() {
        val itemResult1 = BatchDetectEntitiesItemResult().withEntities(
                Entity().withText("The Matrix").withType("Movie"),
                Entity().withText("The Notebook").withType("Movie"))
                .withIndex(0)

        val itemResult2 = BatchDetectEntitiesItemResult().withEntities(
                Entity().withText("The Matrix").withType("Movie"),
                Entity().withText("Titanic").withType("Movie"),
                Entity().withText("Top Boy").withType("Television"))
                .withIndex(1)

        val res = BatchDetectEntitiesResult().withErrorList(BatchItemError()).withResultList(itemResult1, itemResult2)
        val sourceNode1 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))
        val sourceNode2 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 5678L))

        val virtualGraph = AWSVirtualEntitiesGraph(res, listOf(sourceNode1, sourceNode2), RelationshipType { "ENTITY" }).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(6, nodes.size)
        assertThat(nodes, hasItem(sourceNode1))
        assertThat(nodes, hasItem(sourceNode2))

        val matrixNode = VirtualNode(arrayOf(Label{"Movie"}, Label{"Entity"}), mapOf("text" to "The Matrix", "type" to "Movie"))
        val notebookNode = VirtualNode(arrayOf(Label{"Movie"}, Label{"Entity"}), mapOf("text" to "The Notebook", "type" to "Movie"))
        val titanicNode = VirtualNode(arrayOf(Label{"Movie"}, Label{"Entity"}), mapOf("text" to "Titanic", "type" to "Movie"))
        val topBoyNode = VirtualNode(arrayOf(Label{"Television"}, Label{"Entity"}), mapOf("text" to "Top Boy", "type" to "Television"))

        assertThat(nodes, hasItem(NodeMatcher(matrixNode.labels.toList(), matrixNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(notebookNode.labels.toList(), notebookNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(titanicNode.labels.toList(), titanicNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(topBoyNode.labels.toList(), topBoyNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(5, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, matrixNode, "ENTITY")))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, notebookNode, "ENTITY")))

        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, matrixNode, "ENTITY")))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, titanicNode, "ENTITY")))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, topBoyNode, "ENTITY")))
    }

}

