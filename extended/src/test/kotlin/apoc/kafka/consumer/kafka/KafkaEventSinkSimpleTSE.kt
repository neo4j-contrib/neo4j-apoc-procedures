package apoc.kafka.consumer.kafka

import apoc.kafka.events.StreamsPluginStatus
import apoc.kafka.common.support.Assert
import apoc.util.JsonUtil
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.hamcrest.Matchers
import org.junit.Test
import org.neo4j.function.ThrowingSupplier
import org.neo4j.graphdb.Node
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import apoc.ExtendedApocConfig.APOC_KAFKA_ENABLED
import kotlin.test.assertTrue

class KafkaEventSinkSimpleTSE: KafkaEventSinkBaseTSE() {

    private val topics = listOf("shouldWriteCypherQuery")

    @Test
    fun shouldWriteDataFromSink() = runBlocking {
        val db = createDbWithKafkaConfigs(
            "apoc.kafka.sink.topic.cypher.shouldWriteCypherQuery" to cypherQueryTemplate,
            "apoc.kafka.${ConsumerConfig.GROUP_ID_CONFIG}" to "ajeje"
        )

        val producerRecord = ProducerRecord(topics[0], "{\"a\":1}", JsonUtil.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()
        val props = data
                .flatMap {
                    if (it.key == "properties") {
                        val map = it.value as Map<String, Any>
                        map.entries.map { it.key to it.value }
                    } else {
                        listOf(it.key to it.value)
                    }
                }
                .toMap()

        Assert.assertEventually(ThrowingSupplier {
            val query = """
                |MATCH (n:Label) WHERE properties(n) = ${'$'}props
                |RETURN count(*) AS count""".trimMargin()
            db.executeTransactionally(query, mapOf("props" to props)) {
                val result = it.columnAs<Long>("count")
                result.hasNext() && result.next() == 1L && !result.hasNext()
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)

    }

    @Test
    fun shouldNotWriteDataFromSinkWithNoTopicLoaded() = runBlocking {
        val db = createDbWithKafkaConfigs()

        val producerRecord = ProducerRecord(topics[0], "{\"a\":1}", JsonUtil.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()
        delay(5000)

        Assert.assertEventually(ThrowingSupplier {
            val query = """
                |MATCH (n:Label)
                |RETURN n""".trimMargin()
            db.executeTransactionally(query, emptyMap()) {
                val result = it.columnAs<Node>("n")
                result.hasNext()
            }
        }, Matchers.equalTo(false), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `should fix issue 186 with auto commit true`() {
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
            "apoc.kafka.${ConsumerConfig.GROUP_ID_CONFIG}" to "ajeje1"
        )

        val props = mapOf("id" to 1, "name" to "My Awesome Product")
        var producerRecord = ProducerRecord(product.first, "{\"a\":1}",
                JsonUtil.writeValueAsBytes(props))
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

    @Test
    fun `should stop and start the sink via procedures`() = runBlocking {
        // given
        val db = createDbWithKafkaConfigs("apoc.kafka.sink.topic.cypher.shouldWriteCypherQuery" to cypherQueryTemplate)
//        db.setConfig("apoc.kafka.sink.topic.cypher.shouldWriteCypherQuery", cypherQueryTemplate)
        // db.start()
//        db.dependencyResolver.resolveDependency(GlobalProcedures::class.java)
//                .registerProcedure(StreamsSinkProcedures::class.java)

        db.executeTransactionally("CALL apoc.kafka.sink.stop()", emptyMap()) { stopped ->
            assertEquals(mapOf("name" to "status", "value" to StreamsPluginStatus.STOPPED.toString()), stopped.next())
            assertFalse { stopped.hasNext() }
        }

        val producerRecord = ProducerRecord(topics[0], "{\"a\":1}", JsonUtil.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()
        val props = data
                .flatMap {
                    if (it.key == "properties") {
                        val map = it.value as Map<String, Any>
                        map.entries.map { it.key to it.value }
                    } else {
                        listOf(it.key to it.value)
                    }
                }
                .toMap()

        delay(30000)

        val query = """MATCH (n:Label) WHERE properties(n) = ${'$'}props
                |RETURN count(*) AS count""".trimMargin()
        db.executeTransactionally(query, mapOf("props" to props)) {
            val result = it.columnAs<Long>("count")
            assertEquals(0L, result.next())
        }


        // when
        db.executeTransactionally("CALL apoc.kafka.sink.start()", emptyMap()) { started ->
            assertEquals(mapOf("name" to "status", "value" to StreamsPluginStatus.RUNNING.toString()), started.next())
            assertFalse(started.hasNext())
        }

        // then
        Assert.assertEventually(ThrowingSupplier {
            db.executeTransactionally(query, mapOf("props" to props)) {
                val result = it.columnAs<Long>("count")
                if (result.hasNext()) {
                    val next = result.next()
                    println("next = $next")
                    return@executeTransactionally next == 1L && !result.hasNext()
                } 
                return@executeTransactionally false
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun shouldNotStartInASingleInstance() {
        val db = createDbWithKafkaConfigs("apoc.kafka.sink.topic.cypher.shouldWriteCypherQuery" to cypherQueryTemplate,
            "apoc.kafka.cluster.only" to "true")

        val expectedRunning = listOf(mapOf("name" to "status", "value" to StreamsPluginStatus.STOPPED.toString()))

        // when
        val actual = db.executeTransactionally("CALL apoc.kafka.sink.status()", emptyMap()) {
            it.stream().toList()
        }

        // then
        assertEquals(expectedRunning, actual)
    }

    @Test
    fun shouldNotFailWithKafkaEnabledFalse() {
        val db = createDbWithKafkaConfigs(
            "apoc.kafka.sink.topic.cypher.shouldWriteCypherQuery" to cypherQueryTemplate,
            "apoc.kafka.cluster.only" to "true",
            APOC_KAFKA_ENABLED to "false")

        try {
            db.executeTransactionally("CALL apoc.kafka.sink.status()", emptyMap()) {
                it.stream().toList()
            }
        } catch (e: Exception) {
            e.message?.let { assertTrue(it.contains("In order to use the Kafka procedures you must set ${APOC_KAFKA_ENABLED}=true")) }
        }
    }

    @Test
    fun `neo4j should start normally in case kafka is not reachable`() {
        val db = createDbWithKafkaConfigs("apoc.kafka.sink.topic.cypher.shouldWriteCypherQuery" to cypherQueryTemplate,
            "apoc.kafka.bootstrap.servers" to "foo",
            "apoc.kafka.default.api.timeout.ms" to "5000")
//        db.setConfig("apoc.kafka.sink.topic.cypher.shouldWriteCypherQuery", cypherQueryTemplate)
//                .setConfig("apoc.kafka.bootstrap.servers", "foo")
//                .setConfig("apoc.kafka.default.api.timeout.ms", "5000")
//                .start()
//        db.dependencyResolver.resolveDependency(GlobalProcedures::class.java)
//                .registerProcedure(StreamsSinkProcedures::class.java)

        val expectedRunning = listOf(mapOf("name" to "status", "value" to StreamsPluginStatus.STOPPED.toString()))

        // when
        val actual = db.executeTransactionally("CALL apoc.kafka.sink.status()", emptyMap()) {
            it.stream().toList()
        }

        // then
        assertEquals(expectedRunning, actual)
    }
}