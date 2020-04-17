package apoc.nlp.aws

import com.amazonaws.services.comprehend.model.BatchDetectEntitiesItemResult
import com.amazonaws.services.comprehend.model.BatchItemError
import com.amazonaws.services.comprehend.model.Entity
import junit.framework.Assert.assertEquals
import org.junit.Test
import org.mockito.Mockito
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
        val transformedResults = AWSProcedures.transformResults(0, node, resultList, errorList)

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
        val transformedResults = AWSProcedures.transformResults(0, node, resultList, errorList)

        assertEquals(node, transformedResults.node)
        assertEquals(mapOf<String, Any>(), transformedResults.value)

        assertEquals(mapOf("message" to "broken", "code" to "123"), transformedResults.error)
    }

    @Test
    fun `should transform mix of errors and results`() {
        val node1 = Mockito.mock(Node::class.java)
        val node2 = Mockito.mock(Node::class.java)

        val result = BatchDetectEntitiesItemResult()
        val entity = Entity()
        entity.withText("foo").withType("bar").withScore(2.0F).withBeginOffset(0).withEndOffset(3)
        result.withIndex(1).withEntities(listOf(entity))
        val resultList = listOf(result)

        val error = BatchItemError()
        error.withIndex(0).withErrorCode("123").withErrorMessage("broken")
        val errorList = listOf(error)

        val result1 = AWSProcedures.transformResults(0, node1, resultList, errorList)
        assertEquals(node1, result1.node)
        assertEquals(mapOf<String, Any>(), result1.value)
        assertEquals(mapOf("message" to "broken", "code" to "123"), result1.error)

        val result2 = AWSProcedures.transformResults(1, node2, resultList, errorList)
        assertEquals(node2, result2.node)
        assertEquals(mapOf<String, Any>(), result2.error)

        assertEquals(1L, result2.value["index"])
        val entities = result2.value["entities"] as List<Map<String, Object>>
        assertEquals(mapOf("text" to "foo", "type" to "bar", "score" to 2.0F, "beginOffset" to 0L, "endOffset" to 3L), entities[0])
    }

}

