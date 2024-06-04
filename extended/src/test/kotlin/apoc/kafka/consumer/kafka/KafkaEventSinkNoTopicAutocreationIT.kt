package apoc.kafka.consumer.kafka

import apoc.kafka.common.support.KafkaTestUtils
import apoc.kafka.utils.KafkaUtil
import apoc.util.JsonUtil
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.AfterClass
import org.junit.Assume
import org.junit.BeforeClass
import org.junit.Ignore
import org.junit.Test
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Network
import java.util.*
import kotlin.test.assertTrue

@Ignore
class KafkaEventSinkNoTopicAutoCreationIT {
    companion object {
        /**
         * Kafka TestContainers uses Confluent OSS images.
         * We need to keep in mind which is the right Confluent Platform version for the Kafka version this project uses
         *
         * Confluent Platform | Apache Kafka
         *                    |
         * 4.0.x	          | 1.0.x
         * 4.1.x	          | 1.1.x
         * 5.0.x	          | 2.0.x
         *
         * Please see also https://docs.confluent.io/current/installation/versions-interoperability.html#cp-and-apache-kafka-compatibility
         */
        private const val confluentPlatformVersion = "4.0.2"
        @JvmStatic
        lateinit var kafka: KafkaContainer

        @BeforeClass
        @JvmStatic
        fun setUpContainer() {
            var exists = false
            KafkaUtil.ignoreExceptions({
                kafka = KafkaContainer(confluentPlatformVersion)
                    .withNetwork(Network.newNetwork())
                kafka.withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
                kafka.start()
                exists = true
            }, IllegalStateException::class.java)
            Assume.assumeTrue("Kafka container has to exist", exists)
            Assume.assumeTrue("Kafka must be running", ::kafka.isInitialized && kafka.isRunning)
        }

        @AfterClass
        @JvmStatic
        fun tearDownContainer() {
            KafkaUtil.ignoreExceptions({
                kafka.stop()
            }, UninitializedPropertyAccessException::class.java)
        }
    }

    @Test
    fun `should consume only the registered topic`() {
        // given
        val topic = "shouldWriteCypherQuery"
        val client = AdminClient.create(mapOf("bootstrap.servers" to kafka.bootstrapServers))
        val expectedTopics = listOf(topic)
        client.createTopics(expectedTopics.map { NewTopic(it, 1, 1) })
                .all()
                .get()
        val topicList = client.listTopics().names().get()
        val notRegisteredTopic = "notRegistered"
        assertTrue { topicList.containsAll(expectedTopics.toSet()) && !topicList.contains(notRegisteredTopic) }
        // TODO - CAMBIARE CON apoc.conf
//        val db = ImpermanentDbmsRule()
//                .setConfig("apoc.kafka.bootstrap.servers", kafka.bootstrapServers)
//                .setConfig("apoc.kafka.sink.enabled", "true")
//                .setConfig("apoc.kafka.sink.topic.cypher.$notRegisteredTopic", "MERGE (p:NotRegisteredTopic{name: event.name})")
//                .setConfig("apoc.kafka.sink.topic.cypher.$topic", "MERGE (p:Person{name: event.name})")
//                .start()
        val kafkaProducer: KafkaProducer<String, ByteArray> = KafkaTestUtils.createProducer(bootstrapServers = kafka.bootstrapServers)

        // when
        val data = mapOf<String, Any>("name" to "Andrea")
        val producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JsonUtil.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()

        // then
        // todo  - cambiare con apoc.conf
//        Assert.assertEventually(ThrowingSupplier {
//            val count = db.executeTransactionally("MATCH (n:Person) RETURN COUNT(n) AS count", emptyMap()) {
//                it.columnAs<Long>("count")
//                        .next()
//            }
//            val topics = client.listTopics().names().get()
//            count == 1L && !topics.contains(notRegisteredTopic)
//        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
//        kafkaProducer.flush()
//        kafkaProducer.close()
//        client.close()
//        db.shutdownSilently()
    }
}