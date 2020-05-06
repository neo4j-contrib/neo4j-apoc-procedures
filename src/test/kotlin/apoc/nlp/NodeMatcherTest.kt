package apoc.nlp

import apoc.result.VirtualNode
import junit.framework.Assert.assertFalse
import junit.framework.Assert.assertTrue
import org.hamcrest.StringDescription
import org.junit.Test
import org.neo4j.graphdb.Label


class NodeMatcherTest {
    @Test
    fun `different labels`() {
        val labels = listOf(Label { "Person" })
        val properties = mapOf("id" to 1234L)
        val matcher = NodeMatcher(labels, properties)

        assertFalse(matcher.matches(VirtualNode(arrayOf(Label { "Human" }), properties)))
    }

    @Test
    fun `different properties`() {
        val labels = listOf(Label { "Person" })
        val properties = mapOf("id" to 1234L)
        val matcher = NodeMatcher(labels, properties)

        assertFalse(matcher.matches(VirtualNode(labels.toTypedArray(), mapOf("id" to 5678L))))
    }

    @Test
    fun `different labels and properties`() {
        val labels = listOf(Label { "Person" })
        val properties = mapOf("id" to 1234L)
        val matcher = NodeMatcher(labels, properties)

        assertFalse(matcher.matches(VirtualNode(arrayOf(Label { "Human" }), mapOf("id" to 5678L))))
    }

    @Test
    fun `same labels and properties`() {
        val labels = listOf(Label { "Person" })
        val properties = mapOf("id" to 1234L)
        val matcher = NodeMatcher(labels, properties)

        assertTrue(matcher.matches(VirtualNode(labels.toTypedArray(), properties)))
    }

    @Test
    fun `no labels in actual`() {
        val labels = listOf(Label { "Person" })
        val properties = mapOf("id" to 1234L)
        val matcher = NodeMatcher(listOf(), properties)

        assertFalse(matcher.matches(VirtualNode(labels.toTypedArray(), properties)))
    }

    @Test
    fun `no labels in expected`() {
        val labels = listOf(Label { "Person" })
        val properties = mapOf("id" to 1234L)
        val matcher = NodeMatcher(labels, properties)

        assertFalse(matcher.matches(VirtualNode(arrayOf(), properties)))
    }

    @Test
    fun `no labels in actual and expected`() {
        val properties = mapOf("id" to 1234L)
        val matcher = NodeMatcher(listOf(), properties)

        val description = StringDescription()
        matcher.describeTo(description)
        assertTrue(matcher.matches(VirtualNode(arrayOf(), properties)))
    }

    @Test
    fun `no properties in actual`() {
        val labels = listOf(Label { "Person" })
        val properties = mapOf("id" to 1234L)
        val matcher = NodeMatcher(labels, mapOf())

        assertFalse(matcher.matches(VirtualNode(labels.toTypedArray(), properties)))
    }

    @Test
    fun `no properties in expected`() {
        val labels = listOf(Label { "Person" })
        val properties = mapOf("id" to 1234L)
        val matcher = NodeMatcher(labels, properties)

        assertFalse(matcher.matches(VirtualNode(labels.toTypedArray(), mapOf())))
    }

    @Test
    fun `no properties in expected and actual`() {
        val labels = listOf(Label { "Person" })
        val matcher = NodeMatcher(labels, mapOf())

        assertTrue(matcher.matches(VirtualNode(labels.toTypedArray(), mapOf())))
    }

}

