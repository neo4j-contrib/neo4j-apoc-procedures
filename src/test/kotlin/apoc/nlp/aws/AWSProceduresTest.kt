package apoc.nlp.aws

import apoc.result.VirtualNode
import com.amazonaws.services.comprehend.model.BatchDetectEntitiesItemResult
import com.amazonaws.services.comprehend.model.BatchDetectEntitiesResult
import com.amazonaws.services.comprehend.model.BatchItemError
import com.amazonaws.services.comprehend.model.Entity
import junit.framework.Assert.assertEquals
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.hasItem
import org.junit.Test
import org.mockito.Mockito
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node

class AWSProceduresTest {
    @Test
    fun `should transform result`() {
        val node = Mockito.mock(Node::class.java)

        val result = BatchDetectEntitiesItemResult()
        val entity = Entity()
        entity.withText("foo").withType("bar").withScore(2.0F).withBeginOffset(0).withEndOffset(3)
        result.withIndex(0).withEntities(listOf(entity))
        val resultList = listOf(result)

        val errorList = listOf<BatchItemError>()
        val res = BatchDetectEntitiesResult().withErrorList(errorList).withResultList(resultList)
        val transformedResults = AWSProcedures.transformResults(0, node, res)

        assertEquals(node, transformedResults.node)
        assertEquals(mapOf<String, Any>(), transformedResults.error)

        assertEquals(0L, transformedResults.value["index"])
        val entities = transformedResults.value["entities"] as List<Map<String, Object>>
        assertEquals(mapOf("text" to "foo", "type" to "bar", "score" to 2.0F, "beginOffset" to 0L, "endOffset" to 3L), entities[0])
    }

    @Test
    fun `should transform error`() {
        val node = Mockito.mock(Node::class.java)

        val result = BatchDetectEntitiesItemResult()
        val resultList = listOf(result)

        val error = BatchItemError()
        error.withIndex(0).withErrorCode("123").withErrorMessage("broken")
        val errorList = listOf(error)

        val res = BatchDetectEntitiesResult().withErrorList(errorList).withResultList(resultList)
        val transformedResults = AWSProcedures.transformResults(0, node, res)

        assertEquals(node, transformedResults.node)
        assertEquals(mapOf<String, Any>(), transformedResults.value)

        assertEquals(mapOf("message" to "broken", "code" to "123"), transformedResults.error)
    }

    @Test
    fun `should transform mix of errors and results`() {
        val node1 = Mockito.mock(Node::class.java)
        val node2 = Mockito.mock(Node::class.java)

        val entity = Entity().withText("foo").withType("bar").withScore(2.0F).withBeginOffset(0).withEndOffset(3)
        val result = BatchDetectEntitiesItemResult().withIndex(1).withEntities(entity)
        val error = BatchItemError().withIndex(0).withErrorCode("123").withErrorMessage("broken")

        val res = BatchDetectEntitiesResult().withErrorList(error).withResultList(result)
        val result1 = AWSProcedures.transformResults(0, node1, res)
        assertEquals(node1, result1.node)
        assertEquals(mapOf<String, Any>(), result1.value)
        assertEquals(mapOf("message" to "broken", "code" to "123"), result1.error)

        val res2 = BatchDetectEntitiesResult().withErrorList(error).withResultList(result)
        val result2 = AWSProcedures.transformResults(1, node2, res2)
        assertEquals(node2, result2.node)
        assertEquals(mapOf<String, Any>(), result2.error)

        assertEquals(1L, result2.value["index"])
        val entities = result2.value["entities"] as List<Map<String, Object>>
        assertEquals(mapOf("text" to "foo", "type" to "bar", "score" to 2.0F, "beginOffset" to 0L, "endOffset" to 3L), entities[0])
    }

    @Test
    fun `create virtual graph from result with one entity`() {
        val entity = Entity().withText("foo").withType("Bar")
        val itemResult = BatchDetectEntitiesItemResult().withEntities(entity).withIndex(0)
        val res = BatchDetectEntitiesResult().withErrorList(BatchItemError()).withResultList(itemResult)
        val sourceNode = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))

        val virtualGraph = AWSProcedures.virtualGraph(res, sourceNode, mapOf("write" to false), null)

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

        val virtualGraph = AWSProcedures.virtualGraph(res, sourceNode, mapOf("write" to false), null)

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

}

