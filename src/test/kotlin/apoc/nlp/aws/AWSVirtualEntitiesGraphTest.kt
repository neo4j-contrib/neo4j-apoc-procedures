package apoc.nlp.aws

import apoc.nlp.NodeMatcher
import apoc.nlp.RelationshipMatcher
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
        val entity = Entity().withText("foo").withType("Bar").withScore(0.8F)
        val itemResult = BatchDetectEntitiesItemResult().withEntities(entity).withIndex(0)
        val res = BatchDetectEntitiesResult().withErrorList(BatchItemError()).withResultList(itemResult)
        val sourceNode = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))

        val virtualGraph = AWSVirtualEntitiesGraph(res, listOf(sourceNode), RelationshipType { "ENTITY" }, "score", 0.0).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(2, nodes.size)
        assertThat(nodes, hasItem(sourceNode))

        val barLabels = listOf(Label { "Bar" }, Label { "Entity" })
        val barProperties = mapOf("text" to "foo", "type" to "Bar")
        assertThat(nodes, hasItem(NodeMatcher(barLabels, barProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(1, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode, VirtualNode(barLabels.toTypedArray(), barProperties), "ENTITY", mapOf("score" to 0.8F))))
    }

    @Test
    fun `create virtual graph from result with duplicate entities`() {
        val result = BatchDetectEntitiesItemResult().withEntities(
                Entity().withText("The Matrix").withType("Movie").withScore(0.6F),
                Entity().withText("The Matrix").withType("Movie").withScore(0.7F))
                .withIndex(0)

        val res = BatchDetectEntitiesResult().withErrorList(BatchItemError()).withResultList(result)
        val sourceNode = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))

        val virtualGraph = AWSVirtualEntitiesGraph(res, listOf(sourceNode), RelationshipType { "ENTITY" }, "score", 0.0).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(2, nodes.size)
        assertThat(nodes, hasItem(sourceNode))

        val matrixNode = VirtualNode(arrayOf(Label{"Movie"}, Label{"Entity"}), mapOf("text" to "The Matrix", "type" to "Movie"))

        assertThat(nodes, hasItem(NodeMatcher(matrixNode.labels.toList(), matrixNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(1, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode, matrixNode, "ENTITY", mapOf("score" to 0.7F))))
    }

    @Test
    fun `create virtual graph from result with multiple entities`() {
        val result = BatchDetectEntitiesItemResult().withEntities(
                Entity().withText("The Matrix").withType("Movie").withScore(0.6F),
                Entity().withText("The Notebook").withType("Movie").withScore(0.7F))
                .withIndex(0)

        val res = BatchDetectEntitiesResult().withErrorList(BatchItemError()).withResultList(result)
        val sourceNode = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))

        val virtualGraph = AWSVirtualEntitiesGraph(res, listOf(sourceNode), RelationshipType { "ENTITY" }, "score", 0.0).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(3, nodes.size)
        assertThat(nodes, hasItem(sourceNode))

        val matrixNode = VirtualNode(arrayOf(Label{"Movie"}, Label{"Entity"}), mapOf("text" to "The Matrix", "type" to "Movie"))
        val notebookNode = VirtualNode(arrayOf(Label{"Movie"}, Label{"Entity"}), mapOf("text" to "The Notebook", "type" to "Movie"))

        assertThat(nodes, hasItem(NodeMatcher(matrixNode.labels.toList(), matrixNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(notebookNode.labels.toList(), notebookNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(2, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode, matrixNode, "ENTITY", mapOf("score" to 0.6F))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode, notebookNode, "ENTITY", mapOf("score" to 0.7F))))
    }

    @Test
    fun `create virtual graph from result with multiple source nodes`() {
        val itemResult1 = BatchDetectEntitiesItemResult().withEntities(
                Entity().withText("The Matrix").withType("Movie").withScore(0.2F),
                Entity().withText("The Notebook").withType("Movie").withScore(0.3F))
                .withIndex(0)

        val itemResult2 = BatchDetectEntitiesItemResult().withEntities(
                Entity().withText("Toy Story").withType("Movie").withScore(0.4F),
                Entity().withText("Titanic").withType("Movie").withScore(0.5F))
                .withIndex(1)

        val res = BatchDetectEntitiesResult().withErrorList(BatchItemError()).withResultList(itemResult1, itemResult2)
        val sourceNode1 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))
        val sourceNode2 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 5678L))

        val virtualGraph = AWSVirtualEntitiesGraph(res, listOf(sourceNode1, sourceNode2), RelationshipType { "ENTITY" }, "score", 0.0).create()

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
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, matrixNode, "ENTITY", mapOf("score" to 0.2F))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, notebookNode, "ENTITY", mapOf("score" to 0.3F))))

        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, toyStoryNode, "ENTITY", mapOf("score" to 0.4F))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, titanicNode, "ENTITY", mapOf("score" to 0.5F))))
    }

    @Test
    fun `create virtual graph from result with multiple source nodes with overlapping entities`() {
        val itemResult1 = BatchDetectEntitiesItemResult().withEntities(
                Entity().withText("The Matrix").withType("Movie").withScore(0.7F),
                Entity().withText("The Notebook").withType("Movie").withScore(0.8F))
                .withIndex(0)

        val itemResult2 = BatchDetectEntitiesItemResult().withEntities(
                Entity().withText("Titanic").withType("Movie").withScore(0.75F),
                Entity().withText("The Matrix").withType("Movie").withScore(0.9F),
                Entity().withText("Top Boy").withType("Television").withScore(0.4F))
                .withIndex(1)

        val res = BatchDetectEntitiesResult().withErrorList(BatchItemError()).withResultList(itemResult1, itemResult2)
        val sourceNode1 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))
        val sourceNode2 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 5678L))

        val virtualGraph = AWSVirtualEntitiesGraph(res, listOf(sourceNode1, sourceNode2), RelationshipType { "ENTITY" }, "score", 0.0).create()

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
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, matrixNode, "ENTITY", mapOf("score" to 0.7F))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, notebookNode, "ENTITY", mapOf("score" to 0.8F))))

        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, titanicNode, "ENTITY", mapOf("score" to 0.75F))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, matrixNode, "ENTITY", mapOf("score" to 0.9F))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, topBoyNode, "ENTITY", mapOf("score" to 0.4F))))
    }

    @Test
    fun `create graph based on confidence cut off`() {
        val itemResult1 = BatchDetectEntitiesItemResult().withEntities(
                Entity().withText("The Matrix").withType("Movie").withScore(0.7F),
                Entity().withText("The Notebook").withType("Movie").withScore(0.8F))
                .withIndex(0)

        val itemResult2 = BatchDetectEntitiesItemResult().withEntities(
                Entity().withText("Titanic").withType("Movie").withScore(0.75F),
                Entity().withText("The Matrix").withType("Movie").withScore(0.9F),
                Entity().withText("Top Boy").withType("Television").withScore(0.4F))
                .withIndex(1)

        val res = BatchDetectEntitiesResult().withErrorList(BatchItemError()).withResultList(itemResult1, itemResult2)
        val sourceNode1 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))
        val sourceNode2 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 5678L))

        val virtualGraph = AWSVirtualEntitiesGraph(res, listOf(sourceNode1, sourceNode2), RelationshipType { "ENTITY" }, "score", 0.75).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(5, nodes.size)
        assertThat(nodes, hasItem(sourceNode1))
        assertThat(nodes, hasItem(sourceNode2))

        val matrixNode = VirtualNode(arrayOf(Label{"Movie"}, Label{"Entity"}), mapOf("text" to "The Matrix", "type" to "Movie"))
        val notebookNode = VirtualNode(arrayOf(Label{"Movie"}, Label{"Entity"}), mapOf("text" to "The Notebook", "type" to "Movie"))
        val titanicNode = VirtualNode(arrayOf(Label{"Movie"}, Label{"Entity"}), mapOf("text" to "Titanic", "type" to "Movie"))

        assertThat(nodes, hasItem(NodeMatcher(matrixNode.labels.toList(), matrixNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(notebookNode.labels.toList(), notebookNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(titanicNode.labels.toList(), titanicNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(3, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, notebookNode, "ENTITY", mapOf("score" to 0.8F))))

        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, titanicNode, "ENTITY", mapOf("score" to 0.75F))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, matrixNode, "ENTITY", mapOf("score" to 0.9F))))
    }
}

