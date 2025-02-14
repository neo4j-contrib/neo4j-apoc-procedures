package apoc.kafka.common.strategy

import org.junit.Test
import apoc.kafka.service.StreamsSinkEntity
import apoc.kafka.service.sink.strategy.RelationshipPatternConfiguration
import apoc.kafka.service.sink.strategy.RelationshipPatternIngestionStrategy
import apoc.kafka.utils.KafkaUtil
import kotlin.test.assertEquals

class RelationshipPatternIngestionStrategyTest {

    @Test
    fun `should get all properties`() {
        // given
        val startPattern = "LabelA{!idStart}"
        val endPattern = "LabelB{!idEnd}"
        val pattern = "(:$startPattern)-[:REL_TYPE]->(:$endPattern)"
        val config = RelationshipPatternConfiguration.parse(pattern)
        val strategy = RelationshipPatternIngestionStrategy(config)
        val data = mapOf("idStart" to 1, "idEnd" to 2,
                "foo" to "foo",
                "bar" to "bar")

        // when
        val events = listOf(StreamsSinkEntity(data, data))
        val queryEvents = strategy.mergeRelationshipEvents(events)

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
            |${KafkaUtil.UNWIND}
            |MERGE (start:LabelA{idStart: event.start.keys.idStart})
            |SET start = event.start.properties
            |SET start += event.start.keys
            |MERGE (end:LabelB{idEnd: event.end.keys.idEnd})
            |SET end = event.end.properties
            |SET end += event.end.keys
            |MERGE (start)-[r:REL_TYPE]->(end)
            |SET r = event.properties
        """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(mapOf("start" to mapOf("keys" to mapOf("idStart" to 1), "properties" to emptyMap()),
                "end" to mapOf("keys" to mapOf("idEnd" to 2), "properties" to emptyMap()),
                "properties" to mapOf("foo" to "foo", "bar" to "bar"))), queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events))
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events))
        assertEquals(emptyList(), strategy.mergeNodeEvents(events))
    }

    @Test
    fun `should get all properties - simple`() {
        // given
        val startPattern = "LabelA{!idStart}"
        val endPattern = "LabelB{!idEnd}"
        val pattern = "$startPattern REL_TYPE $endPattern"
        val config = RelationshipPatternConfiguration.parse(pattern)
        val strategy = RelationshipPatternIngestionStrategy(config)
        val data = mapOf("idStart" to 1, "idEnd" to 2,
                "foo" to "foo",
                "bar" to "bar")

        // when
        val events = listOf(StreamsSinkEntity(data, data))
        val queryEvents = strategy.mergeRelationshipEvents(events)

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
            |${KafkaUtil.UNWIND}
            |MERGE (start:LabelA{idStart: event.start.keys.idStart})
            |SET start = event.start.properties
            |SET start += event.start.keys
            |MERGE (end:LabelB{idEnd: event.end.keys.idEnd})
            |SET end = event.end.properties
            |SET end += event.end.keys
            |MERGE (start)-[r:REL_TYPE]->(end)
            |SET r = event.properties
        """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(mapOf("start" to mapOf("keys" to mapOf("idStart" to 1), "properties" to emptyMap()),
                "end" to mapOf("keys" to mapOf("idEnd" to 2), "properties" to emptyMap()),
                "properties" to mapOf("foo" to "foo", "bar" to "bar"))), queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events))
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events))
        assertEquals(emptyList(), strategy.mergeNodeEvents(events))
    }

    @Test
    fun `should get all properties with reverse start-end`() {
        // given
        val startPattern = "LabelA{!idStart}"
        val endPattern = "LabelB{!idEnd}"
        val pattern = "(:$endPattern)<-[:REL_TYPE]-(:$startPattern)"
        val config = RelationshipPatternConfiguration.parse(pattern)
        val strategy = RelationshipPatternIngestionStrategy(config)
        val data = mapOf("idStart" to 1, "idEnd" to 2,
                "foo" to "foo",
                "bar" to "bar")

        // when
        val events = listOf(StreamsSinkEntity(data, data))
        val queryEvents = strategy.mergeRelationshipEvents(events)

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
            |${KafkaUtil.UNWIND}
            |MERGE (start:LabelA{idStart: event.start.keys.idStart})
            |SET start = event.start.properties
            |SET start += event.start.keys
            |MERGE (end:LabelB{idEnd: event.end.keys.idEnd})
            |SET end = event.end.properties
            |SET end += event.end.keys
            |MERGE (start)-[r:REL_TYPE]->(end)
            |SET r = event.properties
        """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(mapOf("start" to mapOf("keys" to mapOf("idStart" to 1), "properties" to emptyMap()),
                "end" to mapOf("keys" to mapOf("idEnd" to 2), "properties" to emptyMap()),
                "properties" to mapOf("foo" to "foo", "bar" to "bar"))), queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events))
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events))
        assertEquals(emptyList(), strategy.mergeNodeEvents(events))
    }

    @Test
    fun `should get nested properties`() {
        // given
        val startPattern = "LabelA{!idStart, foo.mapFoo}"
        val endPattern = "LabelB{!idEnd, bar.mapBar}"
        val pattern = "(:$startPattern)-[:REL_TYPE]->(:$endPattern)"
        val config = RelationshipPatternConfiguration.parse(pattern)
        val strategy = RelationshipPatternIngestionStrategy(config)
        val data = mapOf("idStart" to 1, "idEnd" to 2,
                "foo" to mapOf("mapFoo" to "mapFoo"),
                "bar" to mapOf("mapBar" to "mapBar"),
                "rel" to 1,
                "map" to mapOf("a" to "a", "inner" to mapOf("b" to "b")))

        // when
        val events = listOf(StreamsSinkEntity(data, data))
        val queryEvents = strategy.mergeRelationshipEvents(events)

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
            |${KafkaUtil.UNWIND}
            |MERGE (start:LabelA{idStart: event.start.keys.idStart})
            |SET start = event.start.properties
            |SET start += event.start.keys
            |MERGE (end:LabelB{idEnd: event.end.keys.idEnd})
            |SET end = event.end.properties
            |SET end += event.end.keys
            |MERGE (start)-[r:REL_TYPE]->(end)
            |SET r = event.properties
        """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(
                mapOf("start" to mapOf("keys" to mapOf("idStart" to 1), "properties" to mapOf("foo.mapFoo" to "mapFoo")),
                "end" to mapOf("keys" to mapOf("idEnd" to 2), "properties" to mapOf("bar.mapBar" to "mapBar")),
                "properties" to mapOf("rel" to 1, "map.a" to "a", "map.inner.b" to "b"))
        ), queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events))
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events))
        assertEquals(emptyList(), strategy.mergeNodeEvents(events))
    }

    @Test
    fun `should delete the relationship`() {
        // given
        val startPattern = "LabelA{!idStart}"
        val endPattern = "LabelB{!idEnd}"
        val pattern = "(:$startPattern)-[:REL_TYPE]->(:$endPattern)"
        val config = RelationshipPatternConfiguration.parse(pattern)
        val strategy = RelationshipPatternIngestionStrategy(config)
        val data = mapOf("idStart" to 1, "idEnd" to 2,
                "foo" to "foo",
                "bar" to "bar")

        // when
        val events = listOf(StreamsSinkEntity(data, null))
        val queryEvents = strategy.deleteRelationshipEvents(events)

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
            |${KafkaUtil.UNWIND}
            |MATCH (start:LabelA{idStart: event.start.keys.idStart})
            |MATCH (end:LabelB{idEnd: event.end.keys.idEnd})
            |MATCH (start)-[r:REL_TYPE]->(end)
            |DELETE r
        """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(mapOf("start" to mapOf("keys" to mapOf("idStart" to 1), "properties" to emptyMap()),
                "end" to mapOf("keys" to mapOf("idEnd" to 2), "properties" to emptyMap()))), queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events))
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events))
        assertEquals(emptyList(), strategy.mergeNodeEvents(events))
    }

}