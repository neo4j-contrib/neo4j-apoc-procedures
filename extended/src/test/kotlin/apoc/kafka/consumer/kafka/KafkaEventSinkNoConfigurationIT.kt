package apoc.kafka.consumer.kafka

import apoc.ApocConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.junit.After
import org.junit.Ignore
import org.junit.Test
import org.neo4j.test.rule.ImpermanentDbmsRule
import org.testcontainers.containers.GenericContainer
import kotlin.test.assertEquals


class FakeWebServer: GenericContainer<FakeWebServer>("alpine") {
    override fun start() {
        this.withCommand("/bin/sh", "-c", "while true; do { echo -e 'HTTP/1.1 200 OK'; echo ; } | nc -l -p 8000; done")
                .withExposedPorts(8000)
        super.start()
    }

    fun getUrl() = "http://localhost:${getMappedPort(8000)}"
}

@Ignore
class KafkaEventSinkNoConfigurationIT {

    private val topic = "no-config"

    private val db = ImpermanentDbmsRule()

    @After
    fun tearDown() {
//        db.shutdownSilently()
    }

    @Test
    fun `the db should start even with no bootstrap servers provided`() {
        ApocConfig.apocConfig().setProperty("apoc.kafka.bootstrap.servers", "")
        ApocConfig.apocConfig().setProperty("apoc.kafka.sink.enabled", "true")
        ApocConfig.apocConfig().setProperty("apoc.kafka.sink.topic.cypher.$topic", "CREATE (p:Place{name: event.name, coordinates: event.coordinates, citizens: event.citizens})")
        // db.start()
        val count = db.executeTransactionally("MATCH (n) RETURN COUNT(n) AS count", emptyMap()) { it.columnAs<Long>("count").next() }
        assertEquals(0L, count)
    }

    @Test
    fun `the db should start even with AVRO serializers and no schema registry url provided`() {
        val fakeWebServer = FakeWebServer()
        fakeWebServer.start()
        val url = fakeWebServer.getUrl().replace("http://", "")
        ApocConfig.apocConfig().setProperty("apoc.kafka.bootstrap.servers", url)
        ApocConfig.apocConfig().setProperty("apoc.kafka.sink.enabled", "true")
        ApocConfig.apocConfig().setProperty("apoc.kafka.sink.topic.cypher.$topic", "CREATE (p:Place{name: event.name, coordinates: event.coordinates, citizens: event.citizens})")
        ApocConfig.apocConfig().setProperty("apoc.kafka.key.deserializer", KafkaAvroDeserializer::class.java.name)
        ApocConfig.apocConfig().setProperty("apoc.kafka.value.deserializer", KafkaAvroDeserializer::class.java.name)
        // db.start()
        val count = db.executeTransactionally("MATCH (n) RETURN COUNT(n) AS count", emptyMap()) { it.columnAs<Long>("count").next() }
        assertEquals(0L, count)
        fakeWebServer.stop()
    }
}