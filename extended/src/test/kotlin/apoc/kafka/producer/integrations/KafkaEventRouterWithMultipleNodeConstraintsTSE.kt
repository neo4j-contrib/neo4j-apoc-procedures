package apoc.kafka.producer.integrations

import apoc.kafka.events.*
import apoc.kafka.extensions.execute
import apoc.kafka.producer.integrations.KafkaEventRouterTestCommon.initDbWithLogStrategy
import apoc.kafka.common.support.KafkaTestUtils
import apoc.kafka.utils.JSONUtils
import apoc.util.ExtendedTestUtil
import org.apache.kafka.common.config.TopicConfig
import org.junit.Test
import java.time.Duration
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class KafkaEventRouterWithMultipleNodeConstraintsTSE: KafkaEventRouterBaseTSE() {

    @Test
    fun testWithMultipleKeyStrategies() {
        val keyStrategyAll = "BOUGHT"
        val keyStrategyDefault = "ONE_PROP"
        val noKeyStrategy = "DEFAULT"

        val labelStart = "PersonConstr"
        val labelEnd = "ProductConstr"

        val personTopic = UUID.randomUUID().toString()
        val productTopic = UUID.randomUUID().toString()
        val topicWithStrategyAll = UUID.randomUUID().toString()
        val topicWithStrategyDefault = UUID.randomUUID().toString()
        val topicWithoutStrategy = UUID.randomUUID().toString()

        val sourceTopics = mapOf("apoc.kafka.source.topic.nodes.$personTopic" to "$labelStart{*}",
                "apoc.kafka.source.topic.nodes.$productTopic" to "$labelEnd{*}",
                "apoc.kafka.source.topic.relationships.$topicWithStrategyAll" to "$keyStrategyAll{*}",
                "apoc.kafka.source.topic.relationships.$topicWithStrategyDefault" to "$keyStrategyDefault{*}",
                "apoc.kafka.source.topic.relationships.$topicWithoutStrategy" to "$noKeyStrategy{*}",
                "apoc.kafka.source.topic.relationships.$topicWithStrategyAll.key_strategy" to RelKeyStrategy.ALL.toString().toLowerCase(),
                "apoc.kafka.source.topic.relationships.$topicWithStrategyDefault.key_strategy" to RelKeyStrategy.DEFAULT.toString().toLowerCase())
        val queries = listOf("CREATE CONSTRAINT FOR (p:$labelStart) REQUIRE p.surname IS UNIQUE",
                "CREATE CONSTRAINT FOR (p:$labelStart) REQUIRE p.name IS UNIQUE",
                "CREATE CONSTRAINT FOR (p:$labelEnd) REQUIRE p.name IS UNIQUE")


        val db = createDbWithKafkaConfigs("apoc.kafka.source.schema.polling.interval" to "0",
            "apoc.kafka.log.compaction.strategy" to TopicConfig.CLEANUP_POLICY_DELETE,
            "apoc.kafka.source.topic.nodes.$personTopic" to "$labelStart{*}",
            "apoc.kafka.source.topic.nodes.$productTopic" to "$labelEnd{*}",
            "apoc.kafka.source.topic.relationships.$topicWithStrategyAll" to "$keyStrategyAll{*}",
            "apoc.kafka.source.topic.relationships.$topicWithStrategyDefault" to "$keyStrategyDefault{*}",
            "apoc.kafka.source.topic.relationships.$topicWithoutStrategy" to "$noKeyStrategy{*}",
            "apoc.kafka.source.topic.relationships.$topicWithStrategyAll.key_strategy" to RelKeyStrategy.ALL.toString().toLowerCase(),
            "apoc.kafka.source.topic.relationships.$topicWithStrategyDefault.key_strategy" to RelKeyStrategy.DEFAULT.toString().toLowerCase())
        initDbWithLogStrategy(db, TopicConfig.CLEANUP_POLICY_DELETE, sourceTopics, queries)

        val expectedSetConstraints = setOf(
                Constraint(labelStart, setOf("name"), StreamsConstraintType.UNIQUE),
                Constraint(labelStart, setOf("surname"), StreamsConstraintType.UNIQUE),
                Constraint(labelEnd, setOf("name"), StreamsConstraintType.UNIQUE)
        )
        val expectedPropsAllKeyStrategy = mapOf("name" to "Foo", "surname" to "Bar")
        val expectedPropsDefaultKeyStrategy = mapOf("name" to "Foo")
        val expectedEndProps = mapOf("name" to "One")

        // we test key_strategy=all with create/update/delete relationship
        KafkaTestUtils.createConsumer<String, ByteArray>(bootstrapServers = KafkaEventRouterSuiteIT.kafka.bootstrapServers)
                .use { consumer ->
            consumer.subscribe(listOf(topicWithStrategyAll))
            db.execute("""
                |MERGE (p:$labelStart {name:'Foo', surname: 'Bar', address: 'Earth'})
                |MERGE (pp:$labelEnd {name:'One', price: '100€'})
                |MERGE (p)-[:$keyStrategyAll]->(pp)
            """.trimMargin())
            val records = consumer.poll(Duration.ofSeconds(5))
            assertEquals(1, records.count())
            val record = records.first()
            assertNotNull(ExtendedTestUtil.readValue(record.key()))
            val valueCreate = JSONUtils.asStreamsTransactionEvent(record.value())
            val payload = valueCreate.payload as RelationshipPayload
            val (start, end, setConstraints) = Triple(payload.start, payload.end, valueCreate.schema.constraints.toSet())
            assertEquals(start.ids, expectedPropsAllKeyStrategy)
            assertEquals(end.ids, expectedEndProps)
            assertEquals(setConstraints, expectedSetConstraints)
            assertTrue(isValidRelationship(valueCreate, OperationType.created))

            db.execute("MATCH (p)-[rel:$keyStrategyAll]->(pp) SET rel.type = 'update'")
            val updatedRecords = consumer.poll(Duration.ofSeconds(5))
            assertEquals(1, updatedRecords.count())
            val updatedRecord = updatedRecords.first()
            assertNotNull(ExtendedTestUtil.readValue(updatedRecord.key()))
            val valueUpdate = JSONUtils.asStreamsTransactionEvent(updatedRecords.first().value())
            val payloadUpdate = valueUpdate.payload as RelationshipPayload
            val (startUpdate, endUpdate, setConstraintsUpdate) = Triple(payloadUpdate.start, payloadUpdate.end, valueUpdate.schema.constraints.toSet())
            assertEquals(expectedPropsAllKeyStrategy, startUpdate.ids)
            assertEquals(expectedEndProps, endUpdate.ids)
            assertEquals(setConstraintsUpdate, setConstraintsUpdate)
            assertTrue(isValidRelationship(valueUpdate, OperationType.updated))

            db.execute("MATCH (p)-[rel:$keyStrategyAll]->(pp) DELETE rel")
            var deletedRecords = consumer.poll(Duration.ofSeconds(5))
            assertEquals(1, deletedRecords.count())
            val deletedRecord = deletedRecords.first()
            assertNotNull(ExtendedTestUtil.readValue(deletedRecord.key()))
            val valueDelete = JSONUtils.asStreamsTransactionEvent(deletedRecords.first().value())
            val payloadDelete = valueDelete.payload as RelationshipPayload
            val (startDelete, endDelete, setConstraintsDelete) = Triple(payloadDelete.start, payloadDelete.end, valueDelete.schema.constraints.toSet())
            assertEquals(expectedPropsAllKeyStrategy, startDelete.ids)
            assertEquals(expectedEndProps, endDelete.ids)
            assertEquals(expectedSetConstraints, setConstraintsDelete)
            assertTrue(isValidRelationship(valueDelete, OperationType.deleted))
        }

        // we test key_strategy=default with create/update/delete relationship
        KafkaTestUtils.createConsumer<String, ByteArray>(bootstrapServers = KafkaEventRouterSuiteIT.kafka.bootstrapServers)
                .use { consumer ->
            consumer.subscribe(listOf(topicWithStrategyDefault))
            db.execute("""
                |MERGE (p:$labelStart {name:'Foo', surname: 'Bar', address: 'Earth'})
                |MERGE (pp:$labelEnd {name:'One', price: '100€'})
                |MERGE (p)-[:$keyStrategyDefault]->(pp)
            """.trimMargin())
            val records = consumer.poll(Duration.ofSeconds(5))
            assertEquals(1, records.count())
            val record = records.first()
            assertNotNull(ExtendedTestUtil.readValue(record.key()))
            val valueCreate = JSONUtils.asStreamsTransactionEvent(record.value())
            val payload = valueCreate.payload as RelationshipPayload
            val (start, end, setConstraints) = Triple(payload.start, payload.end, valueCreate.schema.constraints.toSet())
            assertEquals(expectedPropsDefaultKeyStrategy, start.ids)
            assertEquals(expectedEndProps, end.ids)
            assertEquals(expectedSetConstraints, setConstraints)
            assertTrue(isValidRelationship(valueCreate, OperationType.created))

            db.execute("MATCH (p)-[rel:$keyStrategyDefault]->(pp) SET rel.type = 'update'")
            val updatedRecords = consumer.poll(Duration.ofSeconds(5))
            assertEquals(1, updatedRecords.count())
            val updatedRecord = updatedRecords.first()
            assertNotNull(ExtendedTestUtil.readValue(updatedRecord.key()))
            val valueUpdate = JSONUtils.asStreamsTransactionEvent(updatedRecords.first().value())
            val payloadUpdate = valueUpdate.payload as RelationshipPayload
            val (startUpdate, endUpdate, setConstraintsUpdate) = Triple(payloadUpdate.start, payloadUpdate.end, valueUpdate.schema.constraints.toSet())
            assertEquals(expectedPropsDefaultKeyStrategy, startUpdate.ids)
            assertEquals(expectedEndProps, endUpdate.ids)
            assertEquals(expectedSetConstraints, setConstraintsUpdate)
            assertTrue(isValidRelationship(valueUpdate, OperationType.updated))

            db.execute("MATCH (p)-[rel:$keyStrategyDefault]->(pp) DELETE rel")
            val deletedRecords = consumer.poll(Duration.ofSeconds(5))
            assertEquals(1, deletedRecords.count())
            val deletedRecord = deletedRecords.first()
            assertNotNull(ExtendedTestUtil.readValue(deletedRecord.key()))
            val valueDelete = JSONUtils.asStreamsTransactionEvent(deletedRecords.first().value())
            val payloadDelete = valueDelete.payload as RelationshipPayload
            val (startDelete, endDelete, setConstraintsDelete) = Triple(payloadDelete.start, payloadDelete.end, valueDelete.schema.constraints.toSet())
            assertEquals(expectedPropsDefaultKeyStrategy, startDelete.ids)
            assertEquals(expectedEndProps, endDelete.ids)
            assertEquals(expectedSetConstraints, setConstraintsDelete)
            assertTrue(isValidRelationship(valueDelete, OperationType.deleted))
        }

        // we test a topic without key_strategy (that is, 'default') with create/update/delete relationship
        KafkaTestUtils.createConsumer<String, ByteArray>(bootstrapServers = KafkaEventRouterSuiteIT.kafka.bootstrapServers)
                .use { consumer ->
            consumer.subscribe(listOf(topicWithoutStrategy))
            db.execute("""
                |MERGE (p:$labelStart {name:'Foo', surname: 'Bar', address: 'Earth'})
                |MERGE (pp:$labelEnd {name:'One', price: '100€'})
                |MERGE (p)-[:$noKeyStrategy]->(pp)
            """.trimMargin())
            val records = consumer.poll(Duration.ofSeconds(5))
            assertEquals(1, records.count())
            val record = records.first()
            assertNotNull(ExtendedTestUtil.readValue(record.key()))
            val valueCreate = JSONUtils.asStreamsTransactionEvent(record.value())
            val payload = valueCreate.payload as RelationshipPayload
            val (start, end, setConstraints) = Triple(payload.start, payload.end, valueCreate.schema.constraints.toSet())
            assertEquals(expectedPropsDefaultKeyStrategy, start.ids)
            assertEquals(expectedEndProps, end.ids)
            assertEquals(expectedSetConstraints, setConstraints)
            assertTrue(isValidRelationship(valueCreate, OperationType.created))

            db.execute("MATCH (p)-[rel:$noKeyStrategy]->(pp) SET rel.type = 'update'")
            val updatedRecords = consumer.poll(Duration.ofSeconds(5))
            assertEquals(1, updatedRecords.count())
            val updatedRecord = updatedRecords.first()
            assertNotNull(ExtendedTestUtil.readValue(updatedRecord.key()))
            val valueUpdate = JSONUtils.asStreamsTransactionEvent(updatedRecords.first().value())
            val payloadUpdate = valueUpdate.payload as RelationshipPayload
            val (startUpdate, endUpdate, setConstraintsUpdate) = Triple(payloadUpdate.start, payloadUpdate.end, valueUpdate.schema.constraints.toSet())
            assertEquals(expectedPropsDefaultKeyStrategy, startUpdate.ids)
            assertEquals(expectedEndProps, endUpdate.ids)
            assertEquals(expectedSetConstraints, setConstraintsUpdate)
            assertTrue(isValidRelationship(valueUpdate, OperationType.updated))

            db.execute("MATCH (p)-[rel:$noKeyStrategy]->(pp) DELETE rel")
            val deletedRecords = consumer.poll(Duration.ofSeconds(5))
            assertEquals(1, deletedRecords.count())
            val deletedRecord = deletedRecords.first()
            assertNotNull(ExtendedTestUtil.readValue(deletedRecord.key()))
            val valueDelete = JSONUtils.asStreamsTransactionEvent(deletedRecords.first().value())
            val payloadDelete = valueDelete.payload as RelationshipPayload
            val (startDelete, endDelete, setConstraintsDelete) = Triple(payloadDelete.start, payloadDelete.end, valueDelete.schema.constraints.toSet())
            assertEquals(expectedPropsDefaultKeyStrategy, startDelete.ids)
            assertEquals(expectedEndProps, endDelete.ids)
            assertEquals(expectedSetConstraints, setConstraintsDelete)
            assertTrue(isValidRelationship(valueDelete, OperationType.deleted))
        }
    }

}