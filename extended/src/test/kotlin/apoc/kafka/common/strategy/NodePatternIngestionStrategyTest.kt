package apoc.kafka.common.strategy

import org.junit.Test
import apoc.kafka.service.StreamsSinkEntity
import apoc.kafka.service.sink.strategy.NodePatternConfiguration
import apoc.kafka.service.sink.strategy.NodePatternIngestionStrategy
import apoc.kafka.utils.KafkaUtil
import kotlin.test.assertEquals

class NodePatternIngestionStrategyTest {

    @Test
    fun `should get all properties`() {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id})")
        val strategy = NodePatternIngestionStrategy(config)
        val data = mapOf("id" to 1, "foo" to "foo", "bar" to "bar", "foobar" to "foobar")

        // when
        val events = listOf(StreamsSinkEntity(data, data))
        val queryEvents = strategy.mergeNodeEvents(events)

        // then
        assertEquals("""
                |${KafkaUtil.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1),
                    "properties" to mapOf("foo" to "foo", "bar" to "bar", "foobar" to "foobar"))
                ),
                queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events))
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events))
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events))
    }

    @Test
    fun `should get nested properties`() {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id, foo.bar})")
        val strategy = NodePatternIngestionStrategy(config)
        val data = mapOf("id" to 1, "foo" to mapOf("bar" to "bar", "foobar" to "foobar"))

        // when
        val events = listOf(StreamsSinkEntity(data, data))
        val queryEvents = strategy.mergeNodeEvents(events)

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
                |${KafkaUtil.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(),
            queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1),
                "properties" to mapOf("foo.bar" to "bar"))),
            queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events))
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events))
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events))
    }

    @Test
    fun `should exclude nested properties`() {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id, -foo})")
        val strategy = NodePatternIngestionStrategy(config)
        val map = mapOf("id" to 1, "foo" to mapOf("bar" to "bar", "foobar" to "foobar"), "prop" to 100)

        // when
        val events = listOf(StreamsSinkEntity(map, map))
        val queryEvents = strategy.mergeNodeEvents(events)

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
                |${KafkaUtil.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(),
                queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1),
                "properties" to mapOf("prop" to 100))),
                queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events))
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events))
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events))
    }

    @Test
    fun `should include nested properties`() {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id, foo})")
        val strategy = NodePatternIngestionStrategy(config)
        val data = mapOf("id" to 1, "foo" to mapOf("bar" to "bar", "foobar" to "foobar"), "prop" to 100)

        // when
        val events = listOf(StreamsSinkEntity(data, data))
        val queryEvents = strategy.mergeNodeEvents(events)

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
                |${KafkaUtil.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(),
                queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1),
                "properties" to mapOf("foo.bar" to "bar", "foo.foobar" to "foobar"))),
                queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events))
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events))
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events))
    }

    @Test
    fun `should exclude the properties`() {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id,-foo,-bar})")
        val strategy = NodePatternIngestionStrategy(config)
        val data = mapOf("id" to 1, "foo" to "foo", "bar" to "bar", "foobar" to "foobar")

        // when
        val events = listOf(StreamsSinkEntity(data, data))
        val queryEvents = strategy.mergeNodeEvents(events)

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
                |${KafkaUtil.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1), "properties" to mapOf("foobar" to "foobar"))), queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events))
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events))
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events))
    }

    @Test
    fun `should include the properties`() {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id,foo,bar})")
        val strategy = NodePatternIngestionStrategy(config)
        val data = mapOf("id" to 1, "foo" to "foo", "bar" to "bar", "foobar" to "foobar")

        // when
        val events = listOf(StreamsSinkEntity(data, data))
        val queryEvents = strategy.mergeNodeEvents(events)

        // then
        assertEquals("""
                |${KafkaUtil.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1), "properties" to mapOf("foo" to "foo", "bar" to "bar"))), queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events))
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events))
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events))
    }

    @Test
    fun `should delete the node`() {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id})")
        val strategy = NodePatternIngestionStrategy(config)
        val data = mapOf("id" to 1, "foo" to "foo", "bar" to "bar", "foobar" to "foobar")

        // when
        val events = listOf(StreamsSinkEntity(data, null))
        val queryEvents = strategy.deleteNodeEvents(events)

        // then
        assertEquals("""
                |${KafkaUtil.UNWIND}
                |MATCH (n:LabelA:LabelB{id: event.keys.id})
                |DETACH DELETE n
            """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1))),
                queryEvents[0].events)
        assertEquals(emptyList(), strategy.mergeNodeEvents(events))
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events))
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events))
    }

}