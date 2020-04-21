package apoc.nlp.aws

import apoc.result.VirtualNode
import apoc.util.TestUtil
import junit.framework.Assert.*
import org.hamcrest.Description
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.hasItem
import org.hamcrest.StringDescription
import org.junit.Assume.assumeTrue
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Test
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.test.rule.ImpermanentDbmsRule
import java.util.*


class NodeMatcherTest {
    @Test
    fun `different labels`() {
        val labels = listOf(Label { "Person" })
        val properties = mapOf("id" to 1234L)
        val matcher = NodeMatcher(labels, properties)

        val description = StringDescription()
        matcher.describeTo(description)
        assertFalse(matcher.matches(VirtualNode(arrayOf(Label {"Human"}), properties)))
    }

    @Test
    fun `different properties`() {
        val labels = listOf(Label { "Person" })
        val properties = mapOf("id" to 1234L)
        val matcher = NodeMatcher(labels, properties)

        val description = StringDescription()
        matcher.describeTo(description)
        assertFalse(matcher.matches(VirtualNode(labels.toTypedArray(), mapOf("id" to 5678L))))
    }

    @Test
    fun `different labels and properties`() {
        val labels = listOf(Label { "Person" })
        val properties = mapOf("id" to 1234L)
        val matcher = NodeMatcher(labels, properties)

        val description = StringDescription()
        matcher.describeTo(description)
        assertFalse(matcher.matches(VirtualNode(arrayOf(Label {"Human"}), mapOf("id" to 5678L))))
    }

    @Test
    fun `same labels and properties`() {
        val labels = listOf(Label { "Person" })
        val properties = mapOf("id" to 1234L)
        val matcher = NodeMatcher(labels, properties)

        val description = StringDescription()
        matcher.describeTo(description)
        assertTrue(matcher.matches(VirtualNode(labels.toTypedArray(), properties)))
    }

    @Test
    fun `no labels in actual`() {
        val labels = listOf(Label { "Person" })
        val properties = mapOf("id" to 1234L)
        val matcher = NodeMatcher(listOf(), properties)

        val description = StringDescription()
        matcher.describeTo(description)
        assertFalse(matcher.matches(VirtualNode(labels.toTypedArray(), properties)))
    }

    @Test
    fun `no labels in expected`() {
        val labels = listOf(Label { "Person" })
        val properties = mapOf("id" to 1234L)
        val matcher = NodeMatcher(labels, properties)

        val description = StringDescription()
        matcher.describeTo(description)
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

        val description = StringDescription()
        matcher.describeTo(description)
        assertFalse(matcher.matches(VirtualNode(labels.toTypedArray(), properties)))
    }

    @Test
    fun `no properties in expected`() {
        val labels = listOf(Label { "Person" })
        val properties = mapOf("id" to 1234L)
        val matcher = NodeMatcher(labels, properties)

        val description = StringDescription()
        matcher.describeTo(description)
        assertFalse(matcher.matches(VirtualNode(labels.toTypedArray(), mapOf())))
    }

    @Test
    fun `no properties in expected and actual`() {
        val labels = listOf(Label { "Person" })
        val matcher = NodeMatcher(labels, mapOf())

        val description = StringDescription()
        matcher.describeTo(description)
        assertTrue(matcher.matches(VirtualNode(labels.toTypedArray(), mapOf())))
    }

}

