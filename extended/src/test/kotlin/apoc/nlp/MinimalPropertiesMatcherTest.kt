package apoc.nlp

import org.junit.Test
import org.junit.jupiter.api.Assertions.*

class MinimalPropertiesMatcherTest {
    @Test
    fun `exact match`() {
        val expected = mapOf("id" to 1234L)
        assertTrue(MinimalPropertiesMatcher(expected).matches(expected))
    }

    @Test
    fun `at least match`() {
        val expected = mapOf("id" to 1234L)
        assertTrue(MinimalPropertiesMatcher(expected).matches(mapOf("id" to 1234L, "name" to "Michael")))
    }

    @Test
    fun `missing items`() {
        val expected = mapOf("id" to 1234L, "name" to "Michael")
        assertFalse(MinimalPropertiesMatcher(expected).matches(mapOf("id" to 1234L)))
    }

    @Test
    fun `different value`() {
        val expected = mapOf("id" to 1234L, "name" to "Michael")
        assertFalse(MinimalPropertiesMatcher(expected).matches(mapOf("id" to 1234L, "name" to "Mark")))
    }
}