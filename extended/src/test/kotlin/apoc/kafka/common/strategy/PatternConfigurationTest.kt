package apoc.kafka.common.strategy

import apoc.kafka.service.sink.strategy.NodePatternConfiguration
import apoc.kafka.service.sink.strategy.PatternConfigurationType
import apoc.kafka.service.sink.strategy.RelationshipPatternConfiguration
import org.junit.Test
import kotlin.test.assertEquals

class NodePatternConfigurationTest {

    @Test
    fun `should extract all params`() {
        // given
        val pattern = "(:LabelA:LabelB{!id,*})"

        // when
        val result = NodePatternConfiguration.parse(pattern)

        // then
        val expected = NodePatternConfiguration(keys = setOf("id"), type = PatternConfigurationType.ALL,
                labels = listOf("LabelA", "LabelB"), properties = emptyList())
        assertEquals(expected, result)
    }

    @Test
    fun `should extract all fixed params`() {
        // given
        val pattern = "(:LabelA{!id,foo,bar})"

        // when
        val result = NodePatternConfiguration.parse(pattern)

        // then
        val expected = NodePatternConfiguration(keys = setOf("id"), type = PatternConfigurationType.INCLUDE,
                labels = listOf("LabelA"), properties = listOf("foo", "bar"))
        assertEquals(expected, result)
    }

    @Test
    fun `should extract complex params`() {
        // given
        val pattern = "(:LabelA{!id,foo.bar})"

        // when
        val result = NodePatternConfiguration.parse(pattern)

        // then
        val expected = NodePatternConfiguration(keys = setOf("id"), type = PatternConfigurationType.INCLUDE,
                labels = listOf("LabelA"), properties = listOf("foo.bar"))
        assertEquals(expected, result)
    }

    @Test
    fun `should extract composite keys with fixed params`() {
        // given
        val pattern = "(:LabelA{!idA,!idB,foo,bar})"

        // when
        val result = NodePatternConfiguration.parse(pattern)

        // then
        val expected = NodePatternConfiguration(keys = setOf("idA", "idB"), type = PatternConfigurationType.INCLUDE,
                labels = listOf("LabelA"), properties = listOf("foo", "bar"))
        assertEquals(expected, result)
    }

    @Test
    fun `should extract all excluded params`() {
        // given
        val pattern = "(:LabelA{!id,-foo,-bar})"

        // when
        val result = NodePatternConfiguration.parse(pattern)

        // then
        val expected = NodePatternConfiguration(keys = setOf("id"), type = PatternConfigurationType.EXCLUDE,
                labels = listOf("LabelA"), properties = listOf("foo", "bar"))
        assertEquals(expected, result)
    }

    @Test(expected = IllegalArgumentException::class)
    fun `should throw an exception because of mixed configuration`() {
        // given
        val pattern = "(:LabelA{!id,-foo,bar})"

        try {
            // when
            NodePatternConfiguration.parse(pattern)
        } catch (e: IllegalArgumentException) {
            // then
            assertEquals("The Node pattern $pattern is not homogeneous", e.message)
            throw e
        }
    }

    @Test(expected = IllegalArgumentException::class)
    fun `should throw an exception because of invalid pattern`() {
        // given
        val pattern = "(LabelA{!id,-foo,bar})"

        try {
            // when
            NodePatternConfiguration.parse(pattern)
        } catch (e: IllegalArgumentException) {
            // then
            assertEquals("The Node pattern $pattern is invalid", e.message)
            throw e
        }
    }

    @Test(expected = IllegalArgumentException::class)
    fun `should throw an exception because the pattern should contains a key`() {
        // given
        val pattern = "(:LabelA{id,-foo,bar})"

        try {
            // when
            NodePatternConfiguration.parse(pattern)
        } catch (e: IllegalArgumentException) {
            // then
            assertEquals("The Node pattern $pattern must contains at lest one key", e.message)
            throw e
        }
    }

    @Test
    fun `should extract all params - simple`() {
        // given
        val pattern = "LabelA:LabelB{!id,*}"

        // when
        val result = NodePatternConfiguration.parse(pattern)

        // then
        val expected = NodePatternConfiguration(keys = setOf("id"), type = PatternConfigurationType.ALL,
                labels = listOf("LabelA", "LabelB"), properties = emptyList())
        assertEquals(expected, result)
    }

    @Test
    fun `should extract all fixed params - simple`() {
        // given
        val pattern = "LabelA{!id,foo,bar}"

        // when
        val result = NodePatternConfiguration.parse(pattern)

        // then
        val expected = NodePatternConfiguration(keys = setOf("id"), type = PatternConfigurationType.INCLUDE,
                labels = listOf("LabelA"), properties = listOf("foo", "bar"))
        assertEquals(expected, result)
    }

    @Test
    fun `should extract complex params - simple`() {
        // given
        val pattern = "LabelA{!id,foo.bar}"

        // when
        val result = NodePatternConfiguration.parse(pattern)

        // then
        val expected = NodePatternConfiguration(keys = setOf("id"), type = PatternConfigurationType.INCLUDE,
                labels = listOf("LabelA"), properties = listOf("foo.bar"))
        assertEquals(expected, result)
    }

    @Test
    fun `should extract composite keys with fixed params - simple`() {
        // given
        val pattern = "LabelA{!idA,!idB,foo,bar}"

        // when
        val result = NodePatternConfiguration.parse(pattern)

        // then
        val expected = NodePatternConfiguration(keys = setOf("idA", "idB"), type = PatternConfigurationType.INCLUDE,
                labels = listOf("LabelA"), properties = listOf("foo", "bar"))
        assertEquals(expected, result)
    }

    @Test
    fun `should extract all excluded params - simple`() {
        // given
        val pattern = "LabelA{!id,-foo,-bar}"

        // when
        val result = NodePatternConfiguration.parse(pattern)

        // then
        val expected = NodePatternConfiguration(keys = setOf("id"), type = PatternConfigurationType.EXCLUDE,
                labels = listOf("LabelA"), properties = listOf("foo", "bar"))
        assertEquals(expected, result)
    }

    @Test(expected = IllegalArgumentException::class)
    fun `should throw an exception because of mixed configuration - simple`() {
        // given
        val pattern = "LabelA{!id,-foo,bar}"

        try {
            // when
            NodePatternConfiguration.parse(pattern)
        } catch (e: IllegalArgumentException) {
            // then
            assertEquals("The Node pattern $pattern is not homogeneous", e.message)
            throw e
        }
    }

    @Test(expected = IllegalArgumentException::class)
    fun `should throw an exception because the pattern should contains a key - simple`() {
        // given
        val pattern = "LabelA{id,-foo,bar}"

        try {
            // when
            NodePatternConfiguration.parse(pattern)
        } catch (e: IllegalArgumentException) {
            // then
            assertEquals("The Node pattern $pattern must contains at lest one key", e.message)
            throw e
        }
    }
}

class RelationshipPatternConfigurationTest {

    @Test
    fun `should extract all params`() {
        // given
        val startPattern = "LabelA{!id,aa}"
        val endPattern = "LabelB{!idB,bb}"
        val pattern = "(:$startPattern)-[:REL_TYPE]->(:$endPattern)"

        // when
        val result = RelationshipPatternConfiguration.parse(pattern)

        // then
        val start = NodePatternConfiguration.parse(startPattern)
        val end = NodePatternConfiguration.parse(endPattern)
        val properties = emptyList<String>()
        val relType = "REL_TYPE"
        val expected = RelationshipPatternConfiguration(start = start, end = end, relType = relType,
                properties = properties, type = PatternConfigurationType.ALL
        )
        assertEquals(expected, result)
    }

    @Test
    fun `should extract all params with reverse source and target`() {
        // given
        val startPattern = "LabelA{!id,aa}"
        val endPattern = "LabelB{!idB,bb}"
        val pattern = "(:$startPattern)<-[:REL_TYPE]-(:$endPattern)"

        // when
        val result = RelationshipPatternConfiguration.parse(pattern)

        // then
        val start = NodePatternConfiguration.parse(startPattern)
        val end = NodePatternConfiguration.parse(endPattern)
        val properties = emptyList<String>()
        val relType = "REL_TYPE"
        val expected = RelationshipPatternConfiguration(start = end, end = start, relType = relType,
                properties = properties, type = PatternConfigurationType.ALL
        )
        assertEquals(expected, result)
    }

    @Test
    fun `should extract all fixed params`() {
        // given
        val startPattern = "LabelA{!id}"
        val endPattern = "LabelB{!idB}"
        val pattern = "(:$startPattern)-[:REL_TYPE{foo, BAR}]->(:$endPattern)"

        // when
        val result = RelationshipPatternConfiguration.parse(pattern)

        // then
        val start = RelationshipPatternConfiguration.getNodeConf(startPattern)
        val end = RelationshipPatternConfiguration.getNodeConf(endPattern)
        val properties = listOf("foo", "BAR")
        val relType = "REL_TYPE"
        val expected = RelationshipPatternConfiguration(start = start, end = end, relType = relType,
                properties = properties, type = PatternConfigurationType.INCLUDE
        )
        assertEquals(expected, result)
    }

    @Test
    fun `should extract complex params`() {
        // given
        val startPattern = "LabelA{!id}"
        val endPattern = "LabelB{!idB}"
        val pattern = "(:$startPattern)-[:REL_TYPE{foo.BAR, BAR.foo}]->(:$endPattern)"

        // when
        val result = RelationshipPatternConfiguration.parse(pattern)

        // then
        val start = RelationshipPatternConfiguration.getNodeConf(startPattern)
        val end = RelationshipPatternConfiguration.getNodeConf(endPattern)
        val properties = listOf("foo.BAR", "BAR.foo")
        val relType = "REL_TYPE"
        val expected = RelationshipPatternConfiguration(start = start, end = end, relType = relType,
                properties = properties, type = PatternConfigurationType.INCLUDE
        )
        assertEquals(expected, result)
    }

    @Test
    fun `should extract all excluded params`() {
        // given
        val startPattern = "LabelA{!id}"
        val endPattern = "LabelB{!idB}"
        val pattern = "(:$startPattern)-[:REL_TYPE{-foo, -BAR}]->(:$endPattern)"

        // when
        val result = RelationshipPatternConfiguration.parse(pattern)

        // then
        val start = RelationshipPatternConfiguration.getNodeConf(startPattern)
        val end = RelationshipPatternConfiguration.getNodeConf(endPattern)
        val properties = listOf("foo", "BAR")
        val relType = "REL_TYPE"
        val expected = RelationshipPatternConfiguration(start = start, end = end, relType = relType,
                properties = properties, type = PatternConfigurationType.EXCLUDE
        )
        assertEquals(expected, result)
    }

    @Test(expected = IllegalArgumentException::class)
    fun `should throw an exception because of mixed configuration`() {
        // given
        val pattern = "(:LabelA{!id})-[:REL_TYPE{foo, -BAR}]->(:LabelB{!idB})"

        try {
            // when
            RelationshipPatternConfiguration.parse(pattern)
        } catch (e: IllegalArgumentException) {
            // then
            assertEquals("The Relationship pattern $pattern is not homogeneous", e.message)
            throw e
        }
    }

    @Test(expected = IllegalArgumentException::class)
    fun `should throw an exception because the pattern should contains nodes with only ids`() {
        // given
        val pattern = "(:LabelA{id})-[:REL_TYPE{foo,BAR}]->(:LabelB{!idB})"

        try {
            // when
            RelationshipPatternConfiguration.parse(pattern)
        } catch (e: IllegalArgumentException) {
            // then
            assertEquals("The Relationship pattern $pattern is invalid", e.message)
            throw e
        }
    }

    @Test(expected = IllegalArgumentException::class)
    fun `should throw an exception because the pattern is invalid`() {
        // given
        val pattern = "(LabelA{!id})-[:REL_TYPE{foo,BAR}]->(:LabelB{!idB})"

        try {
            // when
            RelationshipPatternConfiguration.parse(pattern)
        } catch (e: IllegalArgumentException) {
            // then
            assertEquals("The Relationship pattern $pattern is invalid", e.message)
            throw e
        }
    }

    @Test
    fun `should extract all params - simple`() {
        // given
        val startPattern = "LabelA{!id,aa}"
        val endPattern = "LabelB{!idB,bb}"
        val pattern = "$startPattern REL_TYPE $endPattern"

        // when
        val result = RelationshipPatternConfiguration.parse(pattern)

        // then
        val start = NodePatternConfiguration.parse(startPattern)
        val end = NodePatternConfiguration.parse(endPattern)
        val properties = emptyList<String>()
        val relType = "REL_TYPE"
        val expected = RelationshipPatternConfiguration(start = start, end = end, relType = relType,
                properties = properties, type = PatternConfigurationType.ALL
        )
        assertEquals(expected, result)
    }

    @Test
    fun `should extract all fixed params - simple`() {
        // given
        val startPattern = "LabelA{!id}"
        val endPattern = "LabelB{!idB}"
        val pattern = "$startPattern REL_TYPE{foo, BAR} $endPattern"

        // when
        val result = RelationshipPatternConfiguration.parse(pattern)

        // then
        val start = RelationshipPatternConfiguration.getNodeConf(startPattern)
        val end = RelationshipPatternConfiguration.getNodeConf(endPattern)
        val properties = listOf("foo", "BAR")
        val relType = "REL_TYPE"
        val expected = RelationshipPatternConfiguration(start = start, end = end, relType = relType,
                properties = properties, type = PatternConfigurationType.INCLUDE
        )
        assertEquals(expected, result)
    }

    @Test
    fun `should extract complex params - simple`() {
        // given
        val startPattern = "LabelA{!id}"
        val endPattern = "LabelB{!idB}"
        val pattern = "$startPattern REL_TYPE{foo.BAR, BAR.foo} $endPattern"

        // when
        val result = RelationshipPatternConfiguration.parse(pattern)

        // then
        val start = RelationshipPatternConfiguration.getNodeConf(startPattern)
        val end = RelationshipPatternConfiguration.getNodeConf(endPattern)
        val properties = listOf("foo.BAR", "BAR.foo")
        val relType = "REL_TYPE"
        val expected = RelationshipPatternConfiguration(start = start, end = end, relType = relType,
                properties = properties, type = PatternConfigurationType.INCLUDE
        )
        assertEquals(expected, result)
    }

    @Test
    fun `should extract all excluded params - simple`() {
        // given
        val startPattern = "LabelA{!id}"
        val endPattern = "LabelB{!idB}"
        val pattern = "$startPattern REL_TYPE{-foo, -BAR} $endPattern"

        // when
        val result = RelationshipPatternConfiguration.parse(pattern)

        // then
        val start = RelationshipPatternConfiguration.getNodeConf(startPattern)
        val end = RelationshipPatternConfiguration.getNodeConf(endPattern)
        val properties = listOf("foo", "BAR")
        val relType = "REL_TYPE"
        val expected = RelationshipPatternConfiguration(start = start, end = end, relType = relType,
                properties = properties, type = PatternConfigurationType.EXCLUDE
        )
        assertEquals(expected, result)
    }

    @Test(expected = IllegalArgumentException::class)
    fun `should throw an exception because of mixed configuration - simple`() {
        // given
        val pattern = "LabelA{!id} REL_TYPE{foo, -BAR} LabelB{!idB}"

        try {
            // when
            RelationshipPatternConfiguration.parse(pattern)
        } catch (e: IllegalArgumentException) {
            // then
            assertEquals("The Relationship pattern $pattern is not homogeneous", e.message)
            throw e
        }
    }

    @Test(expected = IllegalArgumentException::class)
    fun `should throw an exception because the pattern should contains nodes with only ids - simple`() {
        // given
        val pattern = "LabelA{id} REL_TYPE{foo,BAR} LabelB{!idB}"

        try {
            // when
            RelationshipPatternConfiguration.parse(pattern)
        } catch (e: IllegalArgumentException) {
            // then
            assertEquals("The Relationship pattern $pattern is invalid", e.message)
            throw e
        }
    }
}