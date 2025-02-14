package apoc.kafka.producer.events

import apoc.kafka.producer.NodeRoutingConfiguration
import apoc.kafka.producer.RelationshipRoutingConfiguration
import apoc.kafka.producer.toMap
import org.junit.Test
import org.mockito.Mockito
import org.neo4j.graphdb.*
import kotlin.test.assertEquals

class StreamsEventBuilderTest {

    @Test
    fun shouldCreateSimpleTypes() {
        // Given
        val payload = "Test"

        // When
        val result = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(payload)
                .build()

        // Then
        assertEquals(payload, result.payload)
    }

    @Test
    fun shouldCreateNode() {
        // Given
        val payload = Mockito.mock(Node::class.java)
        Mockito.`when`(payload.id).thenReturn(1)
        Mockito.`when`(payload.labels).thenReturn(listOf(Label.label("Foo"), Label.label("Bar")))
        Mockito.`when`(payload.allProperties).thenReturn(mapOf("prop" to "foo", "prop1" to "bar"))

        // When
        val result = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(payload)
                .build()

        // Then
        assertEquals(payload.toMap(), result.payload)
    }

    @Test
    fun shouldCreateNodeWithIncludedProperties() {
        // Given
        val nodeRouting = NodeRoutingConfiguration(all = false, labels = listOf("Foo"), include = listOf("prop1"))
        val payload = Mockito.mock(Node::class.java)
        Mockito.`when`(payload.id).thenReturn(1)
        Mockito.`when`(payload.labels).thenReturn(listOf(Label.label("Foo"), Label.label("Bar")))
        Mockito.`when`(payload.allProperties).thenReturn(mapOf("prop" to "foo", "prop1" to "bar"))

        // When
        val result = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(payload)
                .withNodeRoutingConfiguration(nodeRouting)
                .build()

        // Then
        val payloadAsMap = payload.toMap().toMutableMap()
        payloadAsMap["properties"] = payload.allProperties.filter { nodeRouting.include.contains(it.key) }
        val expected = payloadAsMap.toMap()
        assertEquals(expected, result.payload)
    }

    @Test
    fun shouldCreateNodeWithoutExcludedProperties() {
        // Given
        val nodeRouting = NodeRoutingConfiguration(all = false, labels = listOf("Foo"), exclude = listOf("prop1"))
        val payload = Mockito.mock(Node::class.java)
        Mockito.`when`(payload.id).thenReturn(1)
        Mockito.`when`(payload.labels).thenReturn(listOf(Label.label("Foo"), Label.label("Bar")))
        Mockito.`when`(payload.allProperties).thenReturn(mapOf("prop" to "foo", "prop1" to "bar"))

        // When
        val result = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(payload)
                .withNodeRoutingConfiguration(nodeRouting)
                .build()

        // Then
        val payloadAsMap = payload.toMap().toMutableMap()
        payloadAsMap["properties"] = payload.allProperties.filter { !nodeRouting.exclude.contains(it.key) }
        val expected = payloadAsMap.toMap()
        assertEquals(expected, result.payload)
    }

    @Test
    fun shouldCreateRelationship() {
        // Given
        val mockedStartNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedStartNode.id).thenReturn(1)
        Mockito.`when`(mockedStartNode.labels).thenReturn(listOf(Label.label("Foo"), Label.label("Bar")))
        Mockito.`when`(mockedStartNode.allProperties).thenReturn(mapOf("prop" to "foo", "prop1" to "bar"))
        val mockedEndNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedEndNode.id).thenReturn(2)
        Mockito.`when`(mockedEndNode.labels).thenReturn(listOf(Label.label("FooEnd"), Label.label("BarEnd")))
        Mockito.`when`(mockedEndNode.allProperties).thenReturn(mapOf("prop" to "fooEnd", "prop1" to "barEnd"))
        val payload = Mockito.mock(Relationship::class.java)
        Mockito.`when`(payload.id).thenReturn(10)
        Mockito.`when`(payload.type).thenReturn(RelationshipType.withName("KNOWS"))
        Mockito.`when`(payload.startNode).thenReturn(mockedStartNode)
        Mockito.`when`(payload.endNode).thenReturn(mockedEndNode)
        Mockito.`when`(payload.allProperties).thenReturn(mapOf("prop" to "foo", "prop1" to "bar"))

        // When
        val result = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(payload)
                .build()

        // Then
        assertEquals(payload.toMap(), result.payload)
    }

    @Test
    fun shouldCreateRelationshipWithIncludedProperties() {
        // Given
        val relRouting = RelationshipRoutingConfiguration(all = false, name = "KNOWS", include = listOf("prop1"))
        val mockedStartNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedStartNode.id).thenReturn(1)
        Mockito.`when`(mockedStartNode.labels).thenReturn(listOf(Label.label("Foo"), Label.label("Bar")))
        Mockito.`when`(mockedStartNode.allProperties).thenReturn(mapOf("prop" to "foo", "prop1" to "bar"))
        val mockedEndNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedEndNode.id).thenReturn(2)
        Mockito.`when`(mockedEndNode.labels).thenReturn(listOf(Label.label("FooEnd"), Label.label("BarEnd")))
        Mockito.`when`(mockedEndNode.allProperties).thenReturn(mapOf("prop" to "fooEnd", "prop1" to "barEnd"))
        val payload = Mockito.mock(Relationship::class.java)
        Mockito.`when`(payload.id).thenReturn(10)
        Mockito.`when`(payload.type).thenReturn(RelationshipType.withName("KNOWS"))
        Mockito.`when`(payload.startNode).thenReturn(mockedStartNode)
        Mockito.`when`(payload.endNode).thenReturn(mockedEndNode)
        Mockito.`when`(payload.allProperties).thenReturn(mapOf("prop" to "foo", "prop1" to "bar"))

        // When
        val result = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(payload)
                .withRelationshipRoutingConfiguration(relRouting)
                .build()

        // Then
        val payloadAsMap = payload.toMap().toMutableMap()
        payloadAsMap["properties"] = payload.allProperties.filter { relRouting.include.contains(it.key) }
        assertEquals(payloadAsMap.toMap(), result.payload)
    }

    @Test
    fun shouldCreateRelationshipWithoutExcludedProperties() {
        // Given
        val relRouting = RelationshipRoutingConfiguration(all = false, name = "KNOWS", exclude = listOf("prop1"))
        val mockedStartNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedStartNode.id).thenReturn(1)
        Mockito.`when`(mockedStartNode.labels).thenReturn(listOf(Label.label("Foo"), Label.label("Bar")))
        Mockito.`when`(mockedStartNode.allProperties).thenReturn(mapOf("prop" to "foo", "prop1" to "bar"))
        val mockedEndNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedEndNode.id).thenReturn(2)
        Mockito.`when`(mockedEndNode.labels).thenReturn(listOf(Label.label("FooEnd"), Label.label("BarEnd")))
        Mockito.`when`(mockedEndNode.allProperties).thenReturn(mapOf("prop" to "fooEnd", "prop1" to "barEnd"))
        val payload = Mockito.mock(Relationship::class.java)
        Mockito.`when`(payload.id).thenReturn(10)
        Mockito.`when`(payload.type).thenReturn(RelationshipType.withName("KNOWS"))
        Mockito.`when`(payload.startNode).thenReturn(mockedStartNode)
        Mockito.`when`(payload.endNode).thenReturn(mockedEndNode)
        Mockito.`when`(payload.allProperties).thenReturn(mapOf("prop" to "foo", "prop1" to "bar"))

        // When
        val result = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(payload)
                .withRelationshipRoutingConfiguration(relRouting)
                .build()

        // Then
        val payloadAsMap = payload.toMap().toMutableMap()
        payloadAsMap["properties"] = payload.allProperties.filter { !relRouting.exclude.contains(it.key) }
        assertEquals(payloadAsMap.toMap(), result.payload)
    }

    @Test
    fun shouldReturnSimpleMap() {
        // Given
        val payload = mapOf("foo" to "bar", "bar" to 10, "prop" to listOf(1, "two", null, mapOf("foo" to "bar")))

        // When
        val result = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(payload)
                .build()

        // Then
        assertEquals(payload, result.payload)
    }

    @Test
    fun shouldReturnSimpleList() {
        // Given
        val payload = listOf("3", 2, 1, mapOf("foo" to "bar", "bar" to 10, "prop" to listOf(1, "two", null, mapOf("foo" to "bar"))))

        // When
        val result = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(payload)
                .build()

        // Then
        assertEquals(payload, result.payload)
    }

    @Test
    fun shouldReturnMapWithComplexTypes() {
        // Given
        val mockedStartNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedStartNode.id).thenReturn(1)
        Mockito.`when`(mockedStartNode.labels).thenReturn(listOf(Label.label("Foo"), Label.label("Bar")))
        Mockito.`when`(mockedStartNode.allProperties).thenReturn(mapOf("prop" to "foo", "prop1" to "bar"))
        val mockedEndNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedEndNode.id).thenReturn(2)
        Mockito.`when`(mockedEndNode.labels).thenReturn(listOf(Label.label("FooEnd"), Label.label("BarEnd")))
        Mockito.`when`(mockedEndNode.allProperties).thenReturn(mapOf("prop" to "fooEnd", "prop1" to "barEnd"))
        val relationship = Mockito.mock(Relationship::class.java)
        Mockito.`when`(relationship.id).thenReturn(10)
        Mockito.`when`(relationship.type).thenReturn(RelationshipType.withName("KNOWS"))
        Mockito.`when`(relationship.startNode).thenReturn(mockedStartNode)
        Mockito.`when`(relationship.endNode).thenReturn(mockedEndNode)
        Mockito.`when`(relationship.allProperties).thenReturn(mapOf("prop" to "foo", "prop1" to "bar"))

        val node = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedStartNode.id).thenReturn(10)
        Mockito.`when`(mockedStartNode.labels).thenReturn(listOf(Label.label("FooNode"), Label.label("BarNode")))
        Mockito.`when`(mockedStartNode.allProperties).thenReturn(mapOf("prop" to "fooNode", "prop1" to "barNode"))

        val payload = mapOf("node" to node,
                "relationship" to relationship,
                "prop" to listOf(1, "two", null, mapOf("foo" to "bar")))

        // When
        val result = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(payload)
                .build()

        // Then
        val payloadAsMutableMap = payload.toMutableMap()
        payloadAsMutableMap["node"] = (payloadAsMutableMap["node"] as Node).toMap()
        payloadAsMutableMap["relationship"] = (payloadAsMutableMap["relationship"] as Relationship).toMap()
        assertEquals(payloadAsMutableMap.toMap(), result.payload)
    }

    @Test
    fun shouldReturnMapWithComplexTypesFiltered() {
        // Given
        val nodeRouting = NodeRoutingConfiguration(all = false, labels = listOf("Foo"), include = listOf("prop1"))
        val relRouting = RelationshipRoutingConfiguration(all = false, name = "KNOWS", include = listOf("prop1"))
        val mockedStartNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedStartNode.id).thenReturn(1)
        Mockito.`when`(mockedStartNode.labels).thenReturn(listOf(Label.label("Foo"), Label.label("Bar")))
        Mockito.`when`(mockedStartNode.allProperties).thenReturn(mapOf("prop" to "foo", "prop1" to "bar"))
        val mockedEndNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedEndNode.id).thenReturn(2)
        Mockito.`when`(mockedEndNode.labels).thenReturn(listOf(Label.label("FooEnd"), Label.label("BarEnd")))
        Mockito.`when`(mockedEndNode.allProperties).thenReturn(mapOf("prop" to "fooEnd", "prop1" to "barEnd"))
        val relationship = Mockito.mock(Relationship::class.java)
        Mockito.`when`(relationship.id).thenReturn(10)
        Mockito.`when`(relationship.type).thenReturn(RelationshipType.withName("KNOWS"))
        Mockito.`when`(relationship.startNode).thenReturn(mockedStartNode)
        Mockito.`when`(relationship.endNode).thenReturn(mockedEndNode)
        Mockito.`when`(relationship.allProperties).thenReturn(mapOf("prop" to "foo", "prop1" to "bar"))

        val node = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedStartNode.id).thenReturn(10)
        Mockito.`when`(mockedStartNode.labels).thenReturn(listOf(Label.label("FooNode"), Label.label("BarNode")))
        Mockito.`when`(mockedStartNode.allProperties).thenReturn(mapOf("prop" to "fooNode", "prop1" to "barNode"))

        val payload = mapOf("node" to node,
                "relationship" to relationship,
                "prop" to listOf(1, "two", null, mapOf("foo" to "bar")))

        // When
        val resultNode = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(node)
                .withNodeRoutingConfiguration(nodeRouting)
                .build()
        val resultRelationship = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(relationship)
                .withRelationshipRoutingConfiguration(relRouting)
                .build()
        val result = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(payload)
                .withRelationshipRoutingConfiguration(relRouting)
                .withNodeRoutingConfiguration(nodeRouting)
                .build()

        // Then
        val payloadAsMutableMap = payload.toMutableMap()
        payloadAsMutableMap["node"] = resultNode.payload
        payloadAsMutableMap["relationship"] = resultRelationship.payload
        assertEquals(payloadAsMutableMap.toMap(), result.payload)
    }

    @Test
    fun shouldReturnPath() {
        // Given
        val mockedStartNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedStartNode.id).thenReturn(1)
        Mockito.`when`(mockedStartNode.labels).thenReturn(listOf(Label.label("Foo"), Label.label("Bar")))
        Mockito.`when`(mockedStartNode.allProperties).thenReturn(mapOf("prop" to "foo", "prop1" to "bar"))
        val mockedEndNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedEndNode.id).thenReturn(2)
        Mockito.`when`(mockedEndNode.labels).thenReturn(listOf(Label.label("FooEnd"), Label.label("BarEnd")))
        Mockito.`when`(mockedEndNode.allProperties).thenReturn(mapOf("prop" to "fooEnd", "prop1" to "barEnd"))
        val relationship = Mockito.mock(Relationship::class.java)
        Mockito.`when`(relationship.id).thenReturn(10)
        Mockito.`when`(relationship.type).thenReturn(RelationshipType.withName("KNOWS"))
        Mockito.`when`(relationship.startNode).thenReturn(mockedStartNode)
        Mockito.`when`(relationship.endNode).thenReturn(mockedEndNode)
        Mockito.`when`(relationship.allProperties).thenReturn(mapOf("prop" to "foo", "prop1" to "bar"))
        val payload = Mockito.mock(Path::class.java)
        Mockito.`when`(payload.relationships()).thenReturn(listOf(relationship))
        Mockito.`when`(payload.nodes()).thenReturn(listOf(mockedStartNode, mockedEndNode))
        Mockito.`when`(payload.startNode()).thenReturn(mockedStartNode)
        Mockito.`when`(payload.endNode()).thenReturn(mockedEndNode)
        Mockito.`when`(payload.length()).thenReturn(1)

        // When
        val result = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(payload)
                .build()

        // Then
        val nodes = payload.nodes().map { StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(it)
                .build()
                .payload
        }
        val rels = payload.relationships().map { StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(it)
                .build()
                .payload
        }
        val expectedPath = mapOf("length" to 1, "nodes" to nodes, "rels" to rels)
        assertEquals(expectedPath, result.payload)
    }

    @Test
    fun shouldReturnPathWithFilteredProperties() {
        // Given
        val nodeRouting = NodeRoutingConfiguration(all = false, labels = listOf("Foo"), include = listOf("prop1"))
        val relRouting = RelationshipRoutingConfiguration(all = false, name = "KNOWS", include = listOf("prop1"))
        val mockedStartNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedStartNode.id).thenReturn(1)
        Mockito.`when`(mockedStartNode.labels).thenReturn(listOf(Label.label("Foo"), Label.label("Bar")))
        Mockito.`when`(mockedStartNode.allProperties).thenReturn(mapOf("prop" to "foo", "prop1" to "bar"))
        val mockedEndNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedEndNode.id).thenReturn(2)
        Mockito.`when`(mockedEndNode.labels).thenReturn(listOf(Label.label("FooEnd"), Label.label("BarEnd")))
        Mockito.`when`(mockedEndNode.allProperties).thenReturn(mapOf("prop" to "fooEnd", "prop1" to "barEnd"))
        val relationship = Mockito.mock(Relationship::class.java)
        Mockito.`when`(relationship.id).thenReturn(10)
        Mockito.`when`(relationship.type).thenReturn(RelationshipType.withName("KNOWS"))
        Mockito.`when`(relationship.startNode).thenReturn(mockedStartNode)
        Mockito.`when`(relationship.endNode).thenReturn(mockedEndNode)
        Mockito.`when`(relationship.allProperties).thenReturn(mapOf("prop" to "foo", "prop1" to "bar"))
        val payload = Mockito.mock(Path::class.java)
        Mockito.`when`(payload.relationships()).thenReturn(listOf(relationship))
        Mockito.`when`(payload.nodes()).thenReturn(listOf(mockedStartNode, mockedEndNode))
        Mockito.`when`(payload.startNode()).thenReturn(mockedStartNode)
        Mockito.`when`(payload.endNode()).thenReturn(mockedEndNode)
        Mockito.`when`(payload.length()).thenReturn(1)

        // When
        val result = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(payload)
                .withRelationshipRoutingConfiguration(relRouting)
                .withNodeRoutingConfiguration(nodeRouting)
                .build()

        // Then
        val nodes = payload.nodes().map { StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(it)
                .withNodeRoutingConfiguration(nodeRouting)
                .build()
                .payload
        }
        val rels = payload.relationships().map { StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(it)
                .withRelationshipRoutingConfiguration(relRouting)
                .build()
                .payload
        }
        val expectedPath = mapOf("length" to 1, "nodes" to nodes, "rels" to rels)
        assertEquals(expectedPath, result.payload)
    }

}

