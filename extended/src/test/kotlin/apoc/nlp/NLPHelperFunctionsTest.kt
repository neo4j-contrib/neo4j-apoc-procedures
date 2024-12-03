package apoc.nlp

import apoc.result.VirtualNodeExtended
import org.junit.Assert.assertEquals
import org.junit.Test

class NLPHelperFunctionsTest {
    @Test
    fun `should partition sources`() {
        assertEquals(
                listOf(listOf(
                    VirtualNodeExtended(1),
                    VirtualNodeExtended(2),
                    VirtualNodeExtended(3)
                ), listOf(VirtualNodeExtended(4))),
                NLPHelperFunctions.partition(listOf(
                    VirtualNodeExtended(1),
                    VirtualNodeExtended(2),
                    VirtualNodeExtended(3),
                    VirtualNodeExtended(4)
                ), 3)
        )

        assertEquals(
                listOf(listOf(
                    VirtualNodeExtended(1),
                    VirtualNodeExtended(2),
                    VirtualNodeExtended(3)
                )),
                NLPHelperFunctions.partition(listOf(
                    VirtualNodeExtended(1),
                    VirtualNodeExtended(2),
                    VirtualNodeExtended(3)
                ), 3)
        )

        assertEquals(
                listOf(listOf(VirtualNodeExtended(1)), listOf(
                    VirtualNodeExtended(
                        2
                    )
                ), listOf(VirtualNodeExtended(3))),
                NLPHelperFunctions.partition(listOf(
                    VirtualNodeExtended(1),
                    VirtualNodeExtended(2),
                    VirtualNodeExtended(3)
                ), 1)
        )
    }

    @Test
    fun `relationship types`() {
        assertEquals("FOO", NLPHelperFunctions.entityRelationshipType(mapOf("writeRelationshipType" to "FOO", "relationshipType" to "BAR")).name())
        assertEquals("FOO", NLPHelperFunctions.entityRelationshipType(mapOf("relationshipType" to "FOO")).name())
        assertEquals("ENTITY", NLPHelperFunctions.entityRelationshipType(mapOf()).name())

        assertEquals("FOO", NLPHelperFunctions.categoryRelationshipType(mapOf("writeRelationshipType" to "FOO", "relationshipType" to "BAR")).name())
        assertEquals("FOO", NLPHelperFunctions.categoryRelationshipType(mapOf("relationshipType" to "FOO")).name())
        assertEquals("CATEGORY", NLPHelperFunctions.categoryRelationshipType(mapOf()).name())

        assertEquals("FOO", NLPHelperFunctions.keyPhraseRelationshipType(mapOf("writeRelationshipType" to "FOO", "relationshipType" to "BAR")).name())
        assertEquals("FOO", NLPHelperFunctions.keyPhraseRelationshipType(mapOf("relationshipType" to "FOO")).name())
        assertEquals("KEY_PHRASE", NLPHelperFunctions.keyPhraseRelationshipType(mapOf()).name())
    }
}
