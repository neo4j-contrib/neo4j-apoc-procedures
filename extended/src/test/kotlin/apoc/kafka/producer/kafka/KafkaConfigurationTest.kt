package apoc.kafka.producer.kafka

import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class KafkaConfigurationTest {

    @Test
    fun shouldCreateConfiguration() {
        val map = mapOf("apoc.kafka.bootstrap.servers" to "kafka:5678",
                "apoc.kafka.acks" to "10",
                "apoc.kafka.retries" to 1,
                "apoc.kafka.batch.size" to 10,
                "apoc.kafka.buffer.memory" to 1000,
                "apoc.kafka.reindex.batch.size" to 1,
                "apoc.kafka.session.timeout.ms" to 1,
                "apoc.kafka.connection.timeout.ms" to 1,
                "apoc.kafka.replication" to 2,
                "apoc.kafka.transactional.id" to "foo",
                "apoc.kafka.linger.ms" to 10,
                "apoc.kafka.fetch.min.bytes" to 1234,
                "apoc.kafka.topic.discovery.polling.interval" to 0L,
                "apoc.kafka.log.compaction.strategy" to "delete")

        val kafkaConfig = KafkaConfiguration.create(map.mapValues { it.value.toString() })

        assertFalse { kafkaConfig.extraProperties.isEmpty() }
        assertTrue { kafkaConfig.extraProperties.containsKey("fetch.min.bytes") }
        assertEquals(1,  kafkaConfig.extraProperties.size)

        val properties = kafkaConfig.asProperties()

        assertEquals(map["apoc.kafka.bootstrap.servers"], properties["bootstrap.servers"])
        assertEquals(map["apoc.kafka.acks"], properties["acks"])
        assertEquals(map["apoc.kafka.retries"], properties["retries"])
        assertEquals(map["apoc.kafka.batch.size"], properties["batch.size"])
        assertEquals(map["apoc.kafka.buffer.memory"], properties["buffer.memory"])
        assertEquals(map["apoc.kafka.reindex.batch.size"], properties["reindex.batch.size"])
        assertEquals(map["apoc.kafka.session.timeout.ms"], properties["session.timeout.ms"])
        assertEquals(map["apoc.kafka.connection.timeout.ms"], properties["connection.timeout.ms"])
        assertEquals(map["apoc.kafka.replication"], properties["replication"])
        assertEquals(map["apoc.kafka.transactional.id"], properties["transactional.id"])
        assertEquals(map["apoc.kafka.linger.ms"], properties["linger.ms"])
        assertEquals(map["apoc.kafka.fetch.min.bytes"].toString(), properties["fetch.min.bytes"])
        assertEquals(map["apoc.kafka.topic.discovery.polling.interval"], properties["topic.discovery.polling.interval"])
        assertEquals(map["apoc.kafka.log.compaction.strategy"], properties["log.compaction.strategy"])
    }
}