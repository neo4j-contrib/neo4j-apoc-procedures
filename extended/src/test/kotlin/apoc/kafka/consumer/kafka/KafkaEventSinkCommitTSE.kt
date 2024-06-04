package apoc.kafka.consumer.kafka

import apoc.ApocConfig
import apoc.kafka.producer.integrations.KafkaEventSinkSuiteIT
import apoc.kafka.common.support.Assert
import apoc.kafka.common.support.KafkaTestUtils
// import apoc.kafka.support.start
import apoc.kafka.utils.JSONUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.hamcrest.Matchers
import org.junit.Ignore
import org.junit.Test
import org.neo4j.function.ThrowingSupplier
import java.util.*
import java.util.concurrent.TimeUnit

class KafkaEventSinkCommitTSE : KafkaEventSinkBaseTSE() {
    @Test
    fun `should write last offset with auto commit false`() {
        val topic = UUID.randomUUID().toString()

        val db = createDbWithKafkaConfigs(
            "apoc.kafka.sink.topic.cypher.$topic" to cypherQueryTemplate,
            "apoc.kafka.${ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG}" to "false"
        )

        val partition = 0
        var producerRecord = ProducerRecord(topic, partition, "{\"a\":1}", JSONUtils.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()
        val newData = data.toMutableMap()
        newData["id"] = 2
        producerRecord = ProducerRecord(topic, partition, "{\"a\":1}", JSONUtils.writeValueAsBytes(newData))
        val resp = kafkaProducer.send(producerRecord).get()

        Assert.assertEventually(ThrowingSupplier {
            val kafkaConsumer = KafkaTestUtils.createConsumer<String, ByteArray>(
                    bootstrapServers = KafkaEventSinkSuiteIT.kafka.bootstrapServers,
                    schemaRegistryUrl = KafkaEventSinkSuiteIT.schemaRegistry.getSchemaRegistryUrl())
            val offsetAndMetadata = kafkaConsumer.committed(TopicPartition(topic, partition))
            kafkaConsumer.close()
            if (offsetAndMetadata == null) {
                false
            } else {
                val query = "MATCH (n:Label) RETURN count(*) AS count"
                db.executeTransactionally(query, emptyMap()) {
                    val result = it.columnAs<Long>("count")
                    result.hasNext() && result.next() == 2L && !result.hasNext() && resp.offset() + 1 == offsetAndMetadata.offset()
                }
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun shouldWriteLastOffsetWithAsyncCommit() {
        val topic = UUID.randomUUID().toString()

        val db = createDbWithKafkaConfigs(
            "apoc.kafka.sink.topic.cypher.$topic" to cypherQueryTemplate,
            "apoc.kafka.${ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG}" to "false",
            "apoc.kafka.commit.async" to "true"
        )

        val partition = 0
        var producerRecord = ProducerRecord(topic, partition, "{\"a\":1}", JSONUtils.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()
        val newData = data.toMutableMap()
        newData["id"] = 2
        producerRecord = ProducerRecord(topic, partition, "{\"a\":1}", JSONUtils.writeValueAsBytes(newData))
        val resp = kafkaProducer.send(producerRecord).get()

        Assert.assertEventually(ThrowingSupplier {
            val kafkaConsumer = KafkaTestUtils.createConsumer<String, ByteArray>(
                    bootstrapServers = KafkaEventSinkSuiteIT.kafka.bootstrapServers,
                    schemaRegistryUrl = KafkaEventSinkSuiteIT.schemaRegistry.getSchemaRegistryUrl())
            val offsetAndMetadata = kafkaConsumer.committed(TopicPartition(topic, partition))
            kafkaConsumer.close()
            if (offsetAndMetadata == null) {
                false
            } else {
                val query = "MATCH (n:Label) RETURN count(*) AS count"
                db.executeTransactionally(query, emptyMap()) {
                    val result = it.columnAs<Long>("count")
                    result.hasNext() && result.next() == 2L && !result.hasNext() && resp.offset() + 1 == offsetAndMetadata.offset()
                }
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `should fix issue 186 with auto commit false`() {
        val product = "product" to "MERGE (p:Product {id: event.id}) ON CREATE SET p.name = event.name"
        val customer = "customer" to "MERGE (c:Customer {id: event.id}) ON CREATE SET c.name = event.name"
        val bought = "bought" to """
            MERGE (c:Customer {id: event.id})
            MERGE (p:Product {id: event.id})
            MERGE (c)-[:BOUGHT]->(p)
        """.trimIndent()

        val db = createDbWithKafkaConfigs(
            "apoc.kafka.sink.topic.cypher.${product.first}" to product.second,
            "apoc.kafka.sink.topic.cypher.${customer.first}" to customer.second,
            "apoc.kafka.sink.topic.cypher.${bought.first}" to bought.second,
            "apoc.kafka.${ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG}" to "false"
        )

        val props = mapOf("id" to 1, "name" to "My Awesome Product")
        var producerRecord = ProducerRecord(product.first, "{\"a\":1}",
                JSONUtils.writeValueAsBytes(props))
        kafkaProducer.send(producerRecord).get()
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                MATCH (p:Product)
                WHERE properties(p) = ${'$'}props
                RETURN count(p) AS count
            """.trimIndent()
            db.executeTransactionally(query, mapOf("props" to props)) {
                val result = it.columnAs<Long>("count")
                result.hasNext() && result.next() == 1L && !result.hasNext()
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }
}