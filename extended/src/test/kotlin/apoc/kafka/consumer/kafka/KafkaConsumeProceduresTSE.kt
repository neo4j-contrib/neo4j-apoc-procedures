package apoc.kafka.consumer.kafka

import apoc.kafka.common.support.KafkaTestUtils
import apoc.util.JsonUtil
import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.Test
import org.neo4j.graphdb.GraphDatabaseService
import java.util.*
import kotlin.test.*

@Suppress("UNCHECKED_CAST", "DEPRECATION")
class KafkaConsumeProceduresTSE : KafkaEventSinkBaseTSE() {

    private fun testProcedure(db: GraphDatabaseService, topic: String) {
        
        val producerRecord = ProducerRecord(topic, "{\"id\": \"{${UUID.randomUUID()}}\"}", JsonUtil.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()
        db.executeTransactionally("CALL apoc.kafka.consume('$topic', {timeout: 5000}) YIELD event RETURN event", emptyMap()) { result ->
            assertTrue { result.hasNext() }
            val resultMap = result.next()
            assertTrue { resultMap.containsKey("event") }
            assertNotNull(resultMap["event"], "should contain event")
            val event = resultMap["event"] as Map<String, Any?>
            val resultData = event["data"] as Map<String, Any?>
            assertEquals(data, resultData)
        }
    }

    @Test
    fun shouldConsumeDataFromProcedureWithSinkDisabled() {
        val db = createDbWithKafkaConfigs(
            "apoc.kafka.sink.enabled" to "false",
            "apoc.kafka.${ConsumerConfig.GROUP_ID_CONFIG}" to "1"
        )

        val topic = "bar"
        testProcedure(db, topic)
    }

    @Test
    fun shouldConsumeDataFromProcedure() {
        val db = createDbWithKafkaConfigs("apoc.kafka.${ConsumerConfig.GROUP_ID_CONFIG}" to "2")
        val topic = "foo"
        testProcedure(db, topic)
    }

    @Test
    fun shouldTimeout() {
        val db = createDbWithKafkaConfigs()
        db.executeTransactionally("CALL apoc.kafka.consume('foo1', {timeout: 2000}) YIELD event RETURN event", emptyMap()) {
            assertFalse { it.hasNext() }
        }
    }

    @Test
    fun shouldReadSimpleDataType() {
        val db = createDbWithKafkaConfigs("apoc.kafka.${ConsumerConfig.GROUP_ID_CONFIG}" to "3")

        val topic = "simple-data"
        val simpleInt = 1
        val simpleBoolean = true
        val simpleString = "test"
        var producerRecord = ProducerRecord(topic, "{\"a\":1}", JsonUtil.writeValueAsBytes(simpleInt))
        kafkaProducer.send(producerRecord).get()
        producerRecord = ProducerRecord(topic, "{\"a\":2}", JsonUtil.writeValueAsBytes(simpleBoolean))
        kafkaProducer.send(producerRecord).get()
        producerRecord = ProducerRecord(topic, "{\"a\":3}", JsonUtil.writeValueAsBytes(simpleString))
        kafkaProducer.send(producerRecord).get()
        db.executeTransactionally("""
            CALL apoc.kafka.consume('$topic', {timeout: 5000}) YIELD event
            MERGE (t:LOG{simpleData: event.data})
            RETURN count(t) AS insert
        """.trimIndent())
        db.executeTransactionally("""
            MATCH (l:LOG)
            WHERE l.simpleData IN [$simpleInt, $simpleBoolean, "$simpleString"]
            RETURN count(l) as count
        """.trimIndent(), emptyMap()
        ) { searchResult ->
            assertTrue { searchResult.hasNext() }
            val searchResultMap = searchResult.next()
            assertTrue { searchResultMap.containsKey("count") }
            assertEquals(3L, searchResultMap["count"])
        }
    }

    @Test
    fun shouldReadATopicPartitionStartingFromAnOffset() = runBlocking {
        val db = createDbWithKafkaConfigs()

        val topic = "read-from-range"
        val partition = 0
        var start = -1L
        (1..10).forEach {
            val producerRecord = ProducerRecord(topic, partition, "{\"a\":1}", JsonUtil.writeValueAsBytes("{\"b\":${it}}"))
            val recordMetadata = kafkaProducer.send(producerRecord).get()
            if (it == 6) {
                start = recordMetadata.offset()
            }
        }
        delay(3000)
        db.executeTransactionally("""
            CALL apoc.kafka.consume('$topic', {timeout: 5000, partitions: [{partition: $partition, offset: $start}]}) YIELD event
            CREATE (t:LOG{simpleData: event.data})
            RETURN count(t) AS insert
        """.trimIndent())

        val count = db.executeTransactionally("""
            MATCH (l:LOG)
            RETURN count(l) as count
        """.trimIndent(), emptyMap()
        ) {
            it.columnAs<Long>("count").next()
        }
        assertEquals(5L, count)
    }

    @Test
    fun shouldReadFromLatest() = runBlocking {
        val db = createDbWithKafkaConfigs()

        val topic = "simple-data-from-latest"
        val simpleString = "test"
        val partition = 0
        (1..10).forEach {
            val producerRecord = ProducerRecord(topic, partition, "{\"a\":${it}}", JsonUtil.writeValueAsBytes("{\"b\":${it}}"))
            kafkaProducer.send(producerRecord).get()
        }
        delay(1000) // should ignore the three above
        GlobalScope.launch(Dispatchers.IO) {
            delay(1000)
            val producerRecord = ProducerRecord(topic, partition, "{\"a\":1}", JsonUtil.writeValueAsBytes(simpleString))
            kafkaProducer.send(producerRecord).get()
        }
        db.executeTransactionally("""
            CALL apoc.kafka.consume('$topic', {timeout: 5000, from: 'latest', groupId: 'foo'}) YIELD event
            CREATE (t:LOG{simpleData: event.data})
            RETURN count(t) AS insert
        """.trimIndent())
        db.executeTransactionally("""
            MATCH (l:LOG)
            RETURN count(l) AS count
        """.trimIndent(), emptyMap()
        ) { searchResult ->
            assertTrue { searchResult.hasNext() }
            val searchResultMap = searchResult.next()
            assertTrue { searchResultMap.containsKey("count") }
            assertEquals(1L, searchResultMap["count"])
        }
        Unit
    }

    @Test
    fun shouldNotCommit() {
        val db = createDbWithKafkaConfigs(
            "enable.auto.commit" to false,
            "apoc.kafka.${ConsumerConfig.GROUP_ID_CONFIG}" to "ajeje"
        )

        val topic = "simple-data"
        val simpleInt = 1
        val partition = 0
        var producerRecord = ProducerRecord(topic, partition, "{\"a\":1}", JsonUtil.writeValueAsBytes("{\"b\":${simpleInt}}"))
        kafkaProducer.send(producerRecord).get()
        db.executeTransactionally("""
            CALL apoc.kafka.consume('$topic', {timeout: 5000, autoCommit: false, commit:false}) YIELD event
            MERGE (t:LOG{simpleData: event.data})
            RETURN count(t) AS insert
        """.trimIndent())
        db.executeTransactionally("""
            MATCH (l:LOG)
            RETURN count(l) as count
        """.trimIndent(), emptyMap()
        ) { searchResult ->
            assertTrue { searchResult.hasNext() }
            val searchResultMap = searchResult.next()
            assertTrue { searchResultMap.containsKey("count") }
            assertEquals(1L, searchResultMap["count"])
        }
        val kafkaConsumer = KafkaTestUtils.createConsumer<String, ByteArray>(
            bootstrapServers = KafkaEventSinkSuiteIT.kafka.bootstrapServers,
            schemaRegistryUrl = KafkaEventSinkSuiteIT.schemaRegistry.getSchemaRegistryUrl())
        val offsetAndMetadata = kafkaConsumer.committed(TopicPartition(topic, partition))
        assertNull(offsetAndMetadata)
    }
}