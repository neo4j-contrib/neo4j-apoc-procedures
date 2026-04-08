package apoc.kafka.producer

import apoc.kafka.events.*
import apoc.kafka.producer.events.*
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@Suppress("UNCHECKED_CAST")
class RoutingConfigurationTest {

    @Test
    fun badPatternShouldThrowIllegalArgumentException() {
        val topic = "topic1"
        assertIllegalArgumentException(topic, "Label(1,2)", EntityType.node)
        assertIllegalArgumentException(topic, "Label{}", EntityType.node)
        assertIllegalArgumentException(topic, "KNOWS{}", EntityType.relationship)
    }

    private fun assertIllegalArgumentException(topic: String, pattern: String, entityType: EntityType) {
        var hasException = false
        try {
            RoutingConfigurationFactory.getRoutingConfiguration(topic, pattern, entityType)
        } catch (e: Exception) {
            assertTrue { e is IllegalArgumentException }
            assertEquals("The pattern $pattern for topic $topic is invalid", e.message)
            hasException = true
        }
        assertTrue { hasException }
    }

    @Test
    fun shouldCreateNodeRoutingConfiguration() {
        var routing = RoutingConfigurationFactory.getRoutingConfiguration("topic1", "*", EntityType.node) as List<NodeRoutingConfiguration>
        assertEquals(1, routing.size)
        assertEquals("topic1", routing[0].topic)
        assertTrue { routing[0].all }
        assertTrue { routing[0].labels.isEmpty() }
        assertTrue { routing[0].include.isEmpty() }
        assertTrue { routing[0].exclude.isEmpty() }

        routing = RoutingConfigurationFactory.getRoutingConfiguration("topic2", "Label1:Label2{p1,p2}", EntityType.node) as List<NodeRoutingConfiguration>
        assertEquals(1, routing.size)
        assertEquals("topic2", routing[0].topic)
        assertFalse { routing[0].all }
        assertEquals(listOf("Label1","Label2"), routing[0].labels)
        assertEquals(listOf("p1","p2"), routing[0].include)
        assertTrue { routing[0].exclude.isEmpty() }
        
        routing = RoutingConfigurationFactory.getRoutingConfiguration("topic3.1", "Label1;Label2{ p1, p2}", EntityType.node) as List<NodeRoutingConfiguration>
        assertEquals(2, routing.size)
        assertEquals("topic3.1", routing[0].topic)
        assertTrue { routing[0].all }
        assertEquals(listOf("Label1"), routing[0].labels)
        assertTrue { routing[0].include.isEmpty() }
        assertTrue { routing[0].exclude.isEmpty() }
        assertEquals("topic3.1", routing[1].topic)
        assertFalse { routing[1].all }
        assertEquals(listOf("Label2"), routing[1].labels)
        assertEquals(listOf("p1","p2"), routing[1].include)
        assertTrue { routing[1].exclude.isEmpty() }

        routing = RoutingConfigurationFactory.getRoutingConfiguration("topic4", "Label2{ -p1, -p2}", EntityType.node) as List<NodeRoutingConfiguration>
        assertEquals(1, routing.size)
        assertEquals("topic4", routing[0].topic)
        assertFalse { routing[0].all }
        assertEquals(listOf("Label2"), routing[0].labels)
        assertTrue { routing[0].include.isEmpty() }
        assertEquals(listOf("p1","p2"), routing[0].exclude)

        routing = RoutingConfigurationFactory.getRoutingConfiguration("topic5", "Label3{*}", EntityType.node) as List<NodeRoutingConfiguration>
        assertEquals(1, routing.size)
        assertEquals("topic5", routing[0].topic)
        assertTrue { routing[0].all }
        assertEquals(listOf("Label3"), routing[0].labels)
        assertTrue { routing[0].include.isEmpty() }
        assertTrue { routing[0].exclude.isEmpty() }

        routing = RoutingConfigurationFactory.getRoutingConfiguration("topic6", "Label4{ p1,p2,  p3, p4}", EntityType.node) as List<NodeRoutingConfiguration>
        assertEquals(1, routing.size)
        assertEquals("topic6", routing[0].topic)
        assertFalse { routing[0].all }
        assertEquals(listOf("Label4"), routing[0].labels)
        assertTrue { routing[0].exclude.isEmpty() }
        assertEquals(listOf("p1","p2","p3","p4"), routing[0].include)

        routing = RoutingConfigurationFactory.getRoutingConfiguration("topic7", "Label:`labels::label`{ p1,p2,  p3, p4}", EntityType.node) as List<NodeRoutingConfiguration>
        assertEquals(1, routing.size)
        assertEquals("topic7", routing[0].topic)
        assertFalse { routing[0].all }
        assertEquals(listOf("Label", "labels::label"), routing[0].labels)
        assertTrue { routing[0].exclude.isEmpty() }
        assertEquals(listOf("p1","p2","p3","p4"), routing[0].include)

        routing = RoutingConfigurationFactory.getRoutingConfiguration("topic8", " Label  :  ` lorem  : ipsum : dolor : sit `{name, surname}", EntityType.node) as List<NodeRoutingConfiguration>
        assertEquals(1, routing.size)
        assertEquals("topic8", routing[0].topic)
        assertFalse { routing[0].all }
        assertEquals(listOf("Label", " lorem  : ipsum : dolor : sit "), routing[0].labels)
        assertTrue { routing[0].exclude.isEmpty() }
        assertEquals(listOf("name","surname"), routing[0].include)

        routing = RoutingConfigurationFactory.getRoutingConfiguration("topic9", "  `labels::label`:Label:Label1{name, surname}", EntityType.node) as List<NodeRoutingConfiguration>
        assertEquals(1, routing.size)
        assertEquals("topic9", routing[0].topic)
        assertFalse { routing[0].all }
        assertEquals(listOf("labels::label", "Label", "Label1"), routing[0].labels)
        assertTrue { routing[0].exclude.isEmpty() }
        assertEquals(listOf("name","surname"), routing[0].include)

        routing = RoutingConfigurationFactory.getRoutingConfiguration("topic10", ":Label:```labels::label```:Label1{one, two}", EntityType.node) as List<NodeRoutingConfiguration>
        assertEquals(1, routing.size)
        assertEquals("topic10", routing[0].topic)
        assertFalse { routing[0].all }
        assertEquals(listOf("Label", "labels::label", "Label1"), routing[0].labels)
        assertTrue { routing[0].exclude.isEmpty() }
        assertEquals(listOf("one","two"), routing[0].include)

        routing = RoutingConfigurationFactory.getRoutingConfiguration("topic11", ":Label:`labels::label`:`labels1::label1`:Label1{name, surname}", EntityType.node) as List<NodeRoutingConfiguration>
        assertEquals(1, routing.size)
        assertEquals("topic11", routing[0].topic)
        assertFalse { routing[0].all }
        assertEquals(listOf("Label", "labels::label", "labels1::label1", "Label1"), routing[0].labels)
        assertTrue { routing[0].exclude.isEmpty() }
        assertEquals(listOf("name","surname"), routing[0].include)
    }

    @Test
    fun shouldCreateRelationshipRoutingConfiguration() {


        var routing = RoutingConfigurationFactory.getRoutingConfiguration("topic1", "*", EntityType.relationship) as List<RelationshipRoutingConfiguration>
        assertEquals(1, routing.size)
        assertEquals("topic1", routing[0].topic)
        assertEquals(RelKeyStrategy.DEFAULT, routing[0].relKeyStrategy)
        assertTrue { routing[0].all }
        assertTrue { routing[0].name == "" }
        assertTrue { routing[0].include.isEmpty() }
        assertTrue { routing[0].exclude.isEmpty() }


        routing = RoutingConfigurationFactory.getRoutingConfiguration("topic2", "KNOWS", EntityType.relationship) as List<RelationshipRoutingConfiguration>
        assertEquals(1, routing.size)
        assertEquals("topic2", routing[0].topic)
        assertEquals(RelKeyStrategy.DEFAULT, routing[0].relKeyStrategy)
        assertTrue { routing[0].all }
        assertEquals("KNOWS",routing[0].name)
        assertTrue { routing[0].include.isEmpty() }
        assertTrue { routing[0].exclude.isEmpty() }

        routing = RoutingConfigurationFactory.getRoutingConfiguration("topic3", "KNOWS{*}", EntityType.relationship) as List<RelationshipRoutingConfiguration>
        assertEquals(1, routing.size)
        assertEquals("topic3", routing[0].topic)
        assertEquals(RelKeyStrategy.DEFAULT, routing[0].relKeyStrategy)
        assertTrue { routing[0].all }
        assertEquals("KNOWS",routing[0].name)
        assertTrue { routing[0].include.isEmpty() }
        assertTrue { routing[0].exclude.isEmpty() }

        routing = RoutingConfigurationFactory.getRoutingConfiguration("topic4", "KNOWS;LOVES{p1, p2}", EntityType.relationship) as List<RelationshipRoutingConfiguration>
        assertEquals(2, routing.size)
        assertEquals("topic4", routing[0].topic)
        assertEquals(RelKeyStrategy.DEFAULT, routing[0].relKeyStrategy)
        assertTrue { routing[0].all }
        assertEquals("KNOWS",routing[0].name)
        assertTrue { routing[0].include.isEmpty() }
        assertTrue { routing[0].exclude.isEmpty() }
        assertEquals("topic4", routing[1].topic)
        assertFalse { routing[1].all }
        assertEquals("LOVES",routing[1].name)
        assertEquals(listOf("p1","p2"),routing[1].include)
        assertTrue { routing[1].exclude.isEmpty() }

        routing = RoutingConfigurationFactory.getRoutingConfiguration("topic5", "LOVES{-p1, -p2 }", EntityType.relationship) as List<RelationshipRoutingConfiguration>
        assertEquals(1, routing.size)
        assertEquals("topic5", routing[0].topic)
        assertEquals(RelKeyStrategy.DEFAULT, routing[0].relKeyStrategy)
        assertFalse { routing[0].all }
        assertEquals("LOVES",routing[0].name)
        assertTrue { routing[0].include.isEmpty() }
        assertEquals(listOf("p1","p2"),routing[0].exclude)

        routing = RoutingConfigurationFactory.getRoutingConfiguration("topic6", "`KNOWS::VERY:WELL`{one, -two }", EntityType.relationship) as List<RelationshipRoutingConfiguration>
        assertEquals(1, routing.size)
        assertEquals("topic6", routing[0].topic)
        assertFalse { routing[0].all }
        assertEquals("KNOWS::VERY:WELL",routing[0].name)
        assertEquals(listOf("one"),routing[0].include)
        assertEquals(listOf("two"),routing[0].exclude)

        // valid relKeyStrategy ALL
        routing = RoutingConfigurationFactory.getRoutingConfiguration("topic6", "KNOWS{*}", EntityType.relationship, RelKeyStrategy.ALL.toString().toLowerCase()) as List<RelationshipRoutingConfiguration>
        assertEquals(1, routing.size)
        assertEquals("topic6", routing[0].topic)
        assertEquals(RelKeyStrategy.ALL, routing[0].relKeyStrategy)
        assertTrue { routing[0].all }
        assertEquals("KNOWS",routing[0].name)
        assertTrue { routing[0].include.isEmpty() }
        assertTrue { routing[0].exclude.isEmpty() }

        // valid relKeyStrategy DEFAULT
        routing = RoutingConfigurationFactory.getRoutingConfiguration("topic7", "LOVES{-p1, -p2 }", EntityType.relationship, RelKeyStrategy.DEFAULT.toString().toLowerCase()) as List<RelationshipRoutingConfiguration>
        assertEquals(1, routing.size)
        assertEquals("topic7", routing[0].topic)
        assertEquals(RelKeyStrategy.DEFAULT, routing[0].relKeyStrategy)
        assertFalse { routing[0].all }
        assertEquals("LOVES",routing[0].name)
        assertTrue { routing[0].include.isEmpty() }
        assertEquals(listOf("p1","p2"),routing[0].exclude)

        // invalid relKeyStrategy
        routing = RoutingConfigurationFactory.getRoutingConfiguration("topic8", "ANOTHER_ONE{-p1, -p2 }", EntityType.relationship, "Franco") as List<RelationshipRoutingConfiguration>
        assertEquals(1, routing.size)
        assertEquals("topic8", routing[0].topic)
        assertEquals(RelKeyStrategy.DEFAULT, routing[0].relKeyStrategy)
        assertFalse { routing[0].all }
        assertEquals("ANOTHER_ONE",routing[0].name)
        assertTrue { routing[0].include.isEmpty() }
        assertEquals(listOf("p1","p2"),routing[0].exclude)
    }

    @Test(expected = IllegalArgumentException::class)
    fun multipleRelationshipsShouldThrowIllegalArgumentException() {
        RoutingConfigurationFactory.getRoutingConfiguration("topic2", "KNOWS:FAILS", EntityType.relationship)
    }

    @Test
    fun shouldFilterAndRouteNodeEvents() {
        // TODO add more tests like a Label removed
        // Given
        val payload = NodePayloadBuilder()
                .withBefore(NodeChange(properties = mapOf("prop1" to 1, "prop2" to "pippo", "prop3" to 3), labels = listOf("Label1", "Label2")))
                .withAfter(NodeChange(properties = mapOf("prop1" to 1, "prop2" to "pippo", "prop3" to 3, "prop4" to 4), labels = listOf("Label1", "Label2", "Label3 :: Label4")))
                .build()
        val streamsEvent = StreamsTransactionEventBuilder()
                .withMeta(
                    StreamsEventMetaBuilder()
                        .withOperation(OperationType.created)
                        .withTimestamp(System.currentTimeMillis())
                        .withTransactionEventId(1)
                        .withTransactionEventsCount(1)
                        .withUsername("user")
                        .withTransactionId(1)
                        .build())
                .withSchema(SchemaBuilder().withConstraints(emptySet()).withPayload(payload).build())
                .withPayload(payload)
                .build()

        val routingConf = mutableListOf<NodeRoutingConfiguration>()
        routingConf.addAll(RoutingConfigurationFactory.getRoutingConfiguration("topic2", "Label1:Label2{prop1, prop2}", EntityType.node) as List<NodeRoutingConfiguration>)
        routingConf.addAll(RoutingConfigurationFactory.getRoutingConfiguration("topic3", "Label1{*}", EntityType.node) as List<NodeRoutingConfiguration>)
        routingConf.addAll(RoutingConfigurationFactory.getRoutingConfiguration("topic4", "Label2{-prop1}", EntityType.node) as List<NodeRoutingConfiguration>)
        val expectedTopics = setOf("topic3", "topic4", "topic2")

        //When
        val events = NodeRoutingConfiguration.prepareEvent(
                streamsTransactionEvent = streamsEvent,
                routingConf = routingConf)

        // Then
        assertEquals(3, events.size)
        assertTrue { events.keys.containsAll(expectedTopics) }

        assertFalse { events["topic2"]!!.payload.before!!.properties!!.containsKey("prop3") }
        assertFalse { events["topic2"]!!.payload.after!!.properties!!.containsKey("prop3") }
        assertFalse { events["topic2"]!!.payload.after!!.properties!!.containsKey("prop4") }
        var nodeBefore = events["topic2"]!!.payload.before as NodeChange
        var nodeAfter = events["topic2"]!!.payload.after as NodeChange
        assertTrue { nodeBefore.labels!!.toSet().containsAll(setOf("Label1", "Label2")) }
        assertTrue { nodeAfter.labels!!.toSet().containsAll(setOf("Label1", "Label2")) }

        assertTrue { events["topic3"]!!.payload.before!!.properties!!.containsKey("prop1") }
        assertTrue { events["topic3"]!!.payload.before!!.properties!!.containsKey("prop2") }
        assertTrue { events["topic3"]!!.payload.before!!.properties!!.containsKey("prop3") }
        assertTrue { events["topic3"]!!.payload.after!!.properties!!.containsKey("prop1") }
        assertTrue { events["topic3"]!!.payload.after!!.properties!!.containsKey("prop2") }
        assertTrue { events["topic3"]!!.payload.after!!.properties!!.containsKey("prop3") }
        assertTrue { events["topic3"]!!.payload.after!!.properties!!.containsKey("prop4") }
        nodeBefore = events["topic3"]!!.payload.before as NodeChange
        nodeAfter = events["topic3"]!!.payload.after as NodeChange
        assertTrue { nodeBefore.labels!!.toSet().containsAll(setOf("Label1", "Label2")) }
        assertTrue { nodeAfter.labels!!.toSet().containsAll(setOf("Label1", "Label2")) }

        assertFalse { events["topic4"]!!.payload.before!!.properties!!.containsKey("prop1") }
        assertFalse { events["topic4"]!!.payload.after!!.properties!!.containsKey("prop1") }
        nodeBefore = events["topic4"]!!.payload.before as NodeChange
        nodeAfter = events["topic4"]!!.payload.after as NodeChange
        assertTrue { nodeBefore.labels!!.toSet().containsAll(setOf("Label1", "Label2")) }
        assertTrue { nodeAfter.labels!!.toSet().containsAll(setOf("Label1", "Label2")) }
    }

    @Test
    fun shouldFilterAndRouteRelationshipEvents() {
        // Given
        val payload = RelationshipPayloadBuilder()
                .withBefore(RelationshipChange(properties = mapOf("prop1" to 1, "prop2" to "pippo", "prop3" to 3)))
                .withAfter(RelationshipChange(properties = mapOf("prop1" to 1, "prop2" to "pippo", "prop3" to 3, "prop4" to 4)))
                .withStartNode("1", listOf("Label1", "Label2"), emptyMap())
                .withEndNode("2", listOf("Label1", "Label2"), emptyMap())
                .withName("KNOWS")
                .build()
        val streamsEvent = StreamsTransactionEventBuilder()
                .withMeta(StreamsEventMetaBuilder()
                        .withOperation(OperationType.created)
                        .withTimestamp(System.currentTimeMillis())
                        .withTransactionEventId(1)
                        .withTransactionEventsCount(1)
                        .withUsername("user")
                        .withTransactionId(1)
                        .build())
                .withSchema(SchemaBuilder().withConstraints(emptySet()).withPayload(payload).build())
                .withPayload(payload)
                .build()

        val routingConf = mutableListOf<RelationshipRoutingConfiguration>()
        routingConf.addAll(RoutingConfigurationFactory.getRoutingConfiguration("topic2", "KNOWS{prop1, prop2}", EntityType.relationship) as List<RelationshipRoutingConfiguration>)
        routingConf.addAll(RoutingConfigurationFactory.getRoutingConfiguration("topic3", "KNOWS{*}", EntityType.relationship) as List<RelationshipRoutingConfiguration>)
        routingConf.addAll(RoutingConfigurationFactory.getRoutingConfiguration("topic4", "KNOWS{-prop1}", EntityType.relationship) as List<RelationshipRoutingConfiguration>)
        var expectedTopics = setOf("topic3", "topic4", "topic2")

        //When
        val events = RelationshipRoutingConfiguration.prepareEvent(
                streamsTransactionEvent = streamsEvent,
                routingConf = routingConf)

        // Then
        assertEquals(3, events.size)
        assertTrue { events.keys.containsAll(expectedTopics) }

        assertFalse { events["topic2"]!!.payload.before!!.properties!!.containsKey("prop3") }
        assertFalse { events["topic2"]!!.payload.after!!.properties!!.containsKey("prop3") }
        assertFalse { events["topic2"]!!.payload.after!!.properties!!.containsKey("prop4") }

        assertTrue { events["topic3"]!!.payload.before!!.properties!!.containsKey("prop1") }
        assertTrue { events["topic3"]!!.payload.before!!.properties!!.containsKey("prop2") }
        assertTrue { events["topic3"]!!.payload.before!!.properties!!.containsKey("prop3") }
        assertTrue { events["topic3"]!!.payload.after!!.properties!!.containsKey("prop1") }
        assertTrue { events["topic3"]!!.payload.after!!.properties!!.containsKey("prop2") }
        assertTrue { events["topic3"]!!.payload.after!!.properties!!.containsKey("prop3") }
        assertTrue { events["topic3"]!!.payload.after!!.properties!!.containsKey("prop4") }

        assertFalse { events["topic4"]!!.payload.before!!.properties!!.containsKey("prop1") }
        assertFalse { events["topic4"]!!.payload.after!!.properties!!.containsKey("prop1") }

    }

}