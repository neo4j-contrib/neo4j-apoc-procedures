package apoc.nlp.aws

import com.amazonaws.services.comprehend.model.*
import org.junit.Assert.assertEquals
import org.junit.Test
import org.mockito.Mockito
import org.neo4j.graphdb.Node

class AWSProceduresTest {
    @Test
    fun `should transform entity result`() {
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
    fun `should transform entity error`() {
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
    fun `should transform mix of entity errors and results`() {
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
    fun `should transform key phrases result`() {
        val node = Mockito.mock(Node::class.java)

        val result = BatchDetectKeyPhrasesItemResult()
        val keyPhrase = KeyPhrase()
        keyPhrase.withText("foo").withScore(2.0F).withBeginOffset(0).withEndOffset(3)
        result.withIndex(0).withKeyPhrases(listOf(keyPhrase))
        val resultList = listOf(result)

        val errorList = listOf<BatchItemError>()
        val res = BatchDetectKeyPhrasesResult().withErrorList(errorList).withResultList(resultList)
        val transformedResults = AWSProcedures.transformResults(0, node, res)

        assertEquals(node, transformedResults.node)
        assertEquals(mapOf<String, Any>(), transformedResults.error)

        assertEquals(0L, transformedResults.value["index"])
        val entities = transformedResults.value["keyPhrases"] as List<Map<String, Object>>
        assertEquals(mapOf("text" to "foo", "score" to 2.0F, "beginOffset" to 0L, "endOffset" to 3L), entities[0])
    }

    @Test
    fun `should transform key phrases error`() {
        val node = Mockito.mock(Node::class.java)

        val result = BatchDetectKeyPhrasesItemResult()
        val resultList = listOf(result)

        val error = BatchItemError()
        error.withIndex(0).withErrorCode("123").withErrorMessage("broken")
        val errorList = listOf(error)

        val res = BatchDetectKeyPhrasesResult().withErrorList(errorList).withResultList(resultList)
        val transformedResults = AWSProcedures.transformResults(0, node, res)

        assertEquals(node, transformedResults.node)
        assertEquals(mapOf<String, Any>(), transformedResults.value)

        assertEquals(mapOf("message" to "broken", "code" to "123"), transformedResults.error)
    }

    @Test
    fun `should transform mix of key phrase errors and results`() {
        val node1 = Mockito.mock(Node::class.java)
        val node2 = Mockito.mock(Node::class.java)

        val keyPhrase = KeyPhrase().withText("foo").withScore(2.0F).withBeginOffset(0).withEndOffset(3)
        val result = BatchDetectKeyPhrasesItemResult().withIndex(1).withKeyPhrases(keyPhrase)
        val error = BatchItemError().withIndex(0).withErrorCode("123").withErrorMessage("broken")

        val res = BatchDetectKeyPhrasesResult().withErrorList(error).withResultList(result)
        val result1 = AWSProcedures.transformResults(0, node1, res)
        assertEquals(node1, result1.node)
        assertEquals(mapOf<String, Any>(), result1.value)
        assertEquals(mapOf("message" to "broken", "code" to "123"), result1.error)

        val res2 = BatchDetectKeyPhrasesResult().withErrorList(error).withResultList(result)
        val result2 = AWSProcedures.transformResults(1, node2, res2)
        assertEquals(node2, result2.node)
        assertEquals(mapOf<String, Any>(), result2.error)

        assertEquals(1L, result2.value["index"])
        val entities = result2.value["keyPhrases"] as List<Map<String, Object>>
        assertEquals(mapOf("text" to "foo", "score" to 2.0F, "beginOffset" to 0L, "endOffset" to 3L), entities[0])
    }


}

