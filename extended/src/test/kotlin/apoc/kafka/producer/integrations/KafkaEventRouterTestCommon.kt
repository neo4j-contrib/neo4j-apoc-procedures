package apoc.kafka.producer.integrations

import apoc.kafka.extensions.execute
import apoc.kafka.common.support.Assert
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.hamcrest.Matchers
import org.neo4j.function.ThrowingSupplier
import org.neo4j.graphdb.GraphDatabaseService
import java.time.Duration
import java.util.concurrent.TimeUnit

object KafkaEventRouterTestCommon {

    private fun createTopic(topic: String, numTopics: Int, withCompact: Boolean) = run {
        val newTopic = NewTopic(topic, numTopics, 1)
        if (withCompact) {
            newTopic.configs(mapOf(
                    "cleanup.policy" to "compact",
                    "segment.ms" to "10",
                    "retention.ms" to "1",
                    "min.cleanable.dirty.ratio" to "0.01"))
        }
        newTopic
    }

    fun createTopic(topic: String, bootstrapServerMap: Map<String, Any>, numTopics: Int = 1, withCompact: Boolean = true) {
        AdminClient.create(bootstrapServerMap).use {
            val topics = listOf(createTopic(topic, numTopics, withCompact))
            it.createTopics(topics).all().get()
        }
    }

    fun assertTopicFilled(kafkaConsumer: KafkaConsumer<String, ByteArray>,
                          fromBeginning: Boolean = false,
                          timeout: Long = 30,
                          assertion: (ConsumerRecords<String, ByteArray>) -> Boolean = { it.count() == 1 }
        ) {
        Assert.assertEventually(ThrowingSupplier {
            if(fromBeginning) {
                kafkaConsumer.seekToBeginning(kafkaConsumer.assignment())
            }
            val records = kafkaConsumer.poll(Duration.ofSeconds(5))
            assertion(records)
        }, Matchers.equalTo(true), timeout, TimeUnit.SECONDS)
    }

    fun initDbWithLogStrategy(db: GraphDatabaseService, strategy: String, otherConfigs: Map<String, String>? = null, constraints: List<String>? = null) {
        constraints?.forEach { db.execute(it) }
    }
}
