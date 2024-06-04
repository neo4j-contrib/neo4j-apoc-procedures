package apoc.kafka.producer.integrations

import apoc.kafka.events.StreamsEvent
import apoc.kafka.extensions.execute
// import apoc.kafka.support.start
import apoc.kafka.utils.JSONUtils
import apoc.util.ExtendedTestUtil
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.junit.Test
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.graphdb.Result
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.assertNotNull

class KafkaEventRouterProcedureTSE : KafkaEventRouterBaseTSE() {

    @Test
    fun testProcedure() {
        val db = createDbWithKafkaConfigs()

        val topic = UUID.randomUUID().toString()
        KafkaEventRouterSuiteIT.registerPublishProcedure(db)
        kafkaConsumer.subscribe(listOf(topic))
        val message = "Hello World"
        db.execute("CALL apoc.kafka.publish('$topic', '$message')")
        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertTrue { records.all {
            JSONUtils.readValue(it.value(), StreamsEvent::class.java).let {
                message == it.payload
            }
        }}
    }

    @Test
    fun testProcedureWithKey() {
        val db = createDbWithKafkaConfigs()
        val topic = UUID.randomUUID().toString()
        KafkaEventRouterSuiteIT.registerPublishProcedure(db)
        kafkaConsumer.subscribe(listOf(topic))
        val message = "Hello World"
        val keyRecord = "test"
        db.execute("CALL apoc.kafka.publish('$topic', '$message', {key: '$keyRecord'} )")
        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertTrue { records.all {
            JSONUtils.readValue(it.value(), StreamsEvent::class.java).payload == message
            && ExtendedTestUtil.readValue(it.key()) == keyRecord
        }}
    }

    @Test
    fun testProcedureWithKeyAsMap() {
        val db = createDbWithKafkaConfigs()
        val topic = UUID.randomUUID().toString()
        KafkaEventRouterSuiteIT.registerPublishProcedure(db)
        kafkaConsumer.subscribe(listOf(topic))
        val message = "Hello World"
        val keyRecord = mapOf("one" to "Foo", "two" to "Baz", "three" to "Bar")
        db.execute("CALL apoc.kafka.publish('$topic', '$message', {key: \$key } )", mapOf("key" to keyRecord))
        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertTrue { records.all {
            JSONUtils.readValue(it.value(), StreamsEvent::class.java).payload == message
        }}
    }

    @Test
    fun testProcedureWithPartitionAsNotNumber() {
        val db = createDbWithKafkaConfigs()
        // db.start()
        val topic = UUID.randomUUID().toString()
        KafkaEventRouterSuiteIT.registerPublishProcedure(db)
        kafkaConsumer.subscribe(listOf(topic))
        val message = "Hello World"
        val keyRecord = "test"
        val partitionRecord = "notNumber"
        assertFailsWith(QueryExecutionException::class) {
            db.execute("CALL apoc.kafka.publish('$topic', '$message', {key: '$keyRecord', partition: '$partitionRecord' })")
        }
    }

    @Test
    fun testProcedureWithPartitionAndKey() {
        val db = createDbWithKafkaConfigs()
        // db.start()
        val topic = UUID.randomUUID().toString()
        KafkaEventRouterSuiteIT.registerPublishProcedure(db)
        kafkaConsumer.subscribe(listOf(topic))
        val message = "Hello World"
        val keyRecord = "test"
        val partitionRecord = 0
        db.execute("CALL apoc.kafka.publish('$topic', '$message', {key: '$keyRecord', partition: $partitionRecord })")
        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertTrue{ records.all {
            JSONUtils.readValue(it.value(), StreamsEvent::class.java).payload == message
            && ExtendedTestUtil.readValue(it.key()) == keyRecord
            && partitionRecord == it.partition()
        }}
    }

    @Test
    fun testCantPublishNull() {
        val db = createDbWithKafkaConfigs()
        setUpProcedureTests()
        assertFailsWith(RuntimeException::class) {
            db.execute("CALL apoc.kafka.publish('neo4j', null)")
        }
    }

    @Test
    fun testProcedureSyncWithNode() {
        val db = createDbWithKafkaConfigs()
        setUpProcedureTests()
        db.execute("CREATE (n:Baz {age: 23, name: 'Foo', surname: 'Bar'})")

        val recordsCreation = kafkaConsumer.poll(5000)
        assertEquals(1, recordsCreation.count())

        db.execute("MATCH (n:Baz) \n" +
                "CALL apoc.kafka.publish.sync('neo4j', n) \n" +
                "YIELD value \n" +
                "RETURN value") {
            assertSyncResult(it)
        }

        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertEquals(3, ((records.map {
            JSONUtils.readValue(it.value(), StreamsEvent::class.java).payload
        }[0] as Map<String, Any>)["properties"] as Map<String, Any>).size)
    }

    @Test
    fun testProcedureSync() {
        val db = createDbWithKafkaConfigs()
        setUpProcedureTests()
        val message = "Hello World"
        db.execute("CALL apoc.kafka.publish.sync('neo4j', '$message')") {
            assertSyncResult(it)
        }

        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertTrue { records.all {
            JSONUtils.readValue(it.value(), StreamsEvent::class.java).payload == message
        }}
    }


    @Test
    fun testProcedureWithRelationship() {
        val db = createDbWithKafkaConfigs()
        setUpProcedureTests()
        db.execute("CREATE (:Foo {one: 'two'})-[:KNOWS {alpha: 'beta'}]->(:Bar {three: 'four'})")
        val recordsCreation = kafkaConsumer.poll(5000)
        assertEquals(3, recordsCreation.count())

        db.execute("""
            MATCH (:Foo)-[r:KNOWS]->(:Bar)
            |CALL apoc.kafka.publish.sync('neo4j', r)
            |YIELD value RETURN value""".trimMargin()) {
            assertSyncResult(it)
        }
        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())

        val payload = JSONUtils.readValue(records.first().value(), StreamsEvent::class.java).payload as Map<String, Any>
        assertTrue(payload["id"] is String)
        assertEquals(mapOf("alpha" to "beta"), payload["properties"])
        assertEquals("KNOWS", payload["label"])
        assertEquals("relationship", payload["type"])
        val start = payload["start"] as Map<String, Any>
        assertEquals(listOf("Foo"), start["labels"])
        assertEquals(mapOf("one" to "two"), start["properties"])
        assertEquals("node", start["type"])
        val end = payload["end"] as Map<String, Any>
        assertEquals(listOf("Bar"), end["labels"])
        assertEquals(mapOf("three" to "four"), end["properties"])
        assertEquals("node", end["type"])
    }

    @Test
    fun testProcedureSyncWithKeyNull() {
        val db = createDbWithKafkaConfigs()
        setUpProcedureTests()
        db.execute("CREATE (n:Foo {id: 1, name: 'Bar'})")

        val recordsCreation = kafkaConsumer.poll(5000)
        assertEquals(1, recordsCreation.count())

        val message = "Hello World"
        db.execute("MATCH (n:Foo {id: 1}) CALL apoc.kafka.publish.sync('neo4j', '$message', {key: n.foo}) YIELD value RETURN value") { 
            assertSyncResult(it)
        }

        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertTrue { records.all {
            JSONUtils.readValue(it.value(), StreamsEvent::class.java).payload == message
                    && it.key() == null
        }}
    }

    @Test
    fun testProcedureSyncWithConfig() {
        val db = createDbWithKafkaConfigs()
        AdminClient.create(mapOf("bootstrap.servers" to KafkaEventRouterSuiteIT.kafka.bootstrapServers)).use {
            val topic = UUID.randomUUID().toString()

            it.createTopics(listOf(NewTopic(topic, 5, 1)))
                    .all()
                    .get()
            KafkaEventRouterSuiteIT.registerPublishProcedure(db)
            kafkaConsumer.subscribe(listOf(topic))

            val message = "Hello World"
            val keyRecord = "test"
            val partitionRecord = 1
            db.execute("CALL apoc.kafka.publish.sync('$topic', '$message', {key: '$keyRecord', partition: $partitionRecord })") {
                assertSyncResult(it)
            }

            val records = kafkaConsumer.poll(5000)
            assertEquals(1, records.count())
            assertEquals(1, records.count { it.partition() == 1 })
            assertTrue{ records.all {
                JSONUtils.readValue(it.value(), StreamsEvent::class.java).payload == message
                && ExtendedTestUtil.readValue(it.key()) == keyRecord
                && partitionRecord == it.partition()
            }}
        }
    }

    @Test
    fun testProcedureWithTopicWithMultiplePartitionAndKey() {
        val db = createDbWithKafkaConfigs()
        AdminClient.create(mapOf("bootstrap.servers" to KafkaEventRouterSuiteIT.kafka.bootstrapServers)).use {
            val topic = UUID.randomUUID().toString()

            it.createTopics(listOf(NewTopic(topic, 3, 1)))
                    .all()
                    .get()
            KafkaEventRouterSuiteIT.registerPublishProcedure(db)
            kafkaConsumer.subscribe(listOf(topic))

            val message = "Hello World"
            val keyRecord = "test"
            val partitionRecord = 2
            db.execute("CALL apoc.kafka.publish('$topic', '$message', {key: '$keyRecord', partition: $partitionRecord })")

            val records = kafkaConsumer.poll(5000)
            assertEquals(1, records.count())
            assertEquals(1, records.count { it.partition() == 2 })
            assertTrue{ records.all {
                JSONUtils.readValue(it.value(), StreamsEvent::class.java).payload == message
                && ExtendedTestUtil.readValue(it.key()) == keyRecord
                && partitionRecord == it.partition()
            }}
        }
    }

    @Test
    fun testProcedureSendMessageToNotExistentPartition() {
        val db = createDbWithKafkaConfigs()
        AdminClient.create(mapOf("bootstrap.servers" to KafkaEventRouterSuiteIT.kafka.bootstrapServers)).use {
            val topic = UUID.randomUUID().toString()

            it.createTopics(listOf(NewTopic(topic, 3, 1)))
                    .all()
                    .get()
            KafkaEventRouterSuiteIT.registerPublishProcedure(db)
            kafkaConsumer.subscribe(listOf(topic))

            val message = "Hello World"
            val keyRecord = "test"
            val partitionRecord = 9
            db.execute("CALL apoc.kafka.publish('$topic', '$message', {key: '$keyRecord', partition: $partitionRecord })")

            val records = kafkaConsumer.poll(5000)
            assertEquals(0, records.count())
        }
    }

    private fun setUpProcedureTests() {
        kafkaConsumer.subscribe(listOf("neo4j"))
    }

    private fun assertSyncResult(it: Result) {
        assertTrue { it.hasNext() }
        val resultMap = (it.next())["value"] as Map<String, Any>
        assertNotNull(resultMap["offset"])
        assertNotNull(resultMap["partition"])
        assertNotNull(resultMap["keySize"])
        assertNotNull(resultMap["valueSize"])
        assertNotNull(resultMap["timestamp"])
        assertFalse { it.hasNext() }
    }
}