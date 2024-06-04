package apoc.kafka.producer.integrations

import apoc.kafka.events.*
import apoc.kafka.extensions.execute
import apoc.kafka.producer.integrations.KafkaEventRouterTestCommon.initDbWithLogStrategy
import apoc.kafka.utils.JSONUtils
import org.apache.kafka.common.config.TopicConfig
import org.junit.Before
import org.junit.Test
import java.util.*
import kotlin.test.assertEquals

class KafkaEventRouterWithConstraintsTSE: KafkaEventRouterBaseTSE() {
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

    @Before
    fun setUpInner() {
        val sourceTopics = mapOf("apoc.kafka.source.topic.nodes.$personTopic" to "$labelStart{*}",
            "apoc.kafka.source.topic.nodes.$productTopic" to "$labelEnd{*}",
            "apoc.kafka.source.topic.relationships.$topicWithStrategyAll" to "$keyStrategyAll{*}",
            "apoc.kafka.source.topic.relationships.$topicWithStrategyDefault" to "$keyStrategyDefault{*}",
            "apoc.kafka.source.topic.relationships.$topicWithoutStrategy" to "$noKeyStrategy{*}",
            "apoc.kafka.source.topic.relationships.$topicWithStrategyAll.key_strategy" to RelKeyStrategy.ALL.toString().toLowerCase(),
            "apoc.kafka.source.topic.relationships.$topicWithStrategyDefault.key_strategy" to RelKeyStrategy.DEFAULT.toString().toLowerCase())

        db = createDbWithKafkaConfigs("apoc.kafka.source.schema.polling.interval" to "0",
            "apoc.kafka.log.compaction.strategy" to TopicConfig.CLEANUP_POLICY_DELETE,
            "apoc.kafka.source.topic.nodes.personConstraints" to "PersonConstr{*}",
            "apoc.kafka.source.topic.nodes.productConstraints" to "ProductConstr{*}",
            "apoc.kafka.source.topic.relationships.boughtConstraints" to "BOUGHT{*}"
            )

        val queries = listOf("CREATE CONSTRAINT FOR (p:$labelStart) REQUIRE p.name IS UNIQUE",
            "CREATE CONSTRAINT FOR (p:$labelEnd) REQUIRE p.name IS UNIQUE")

        initDbWithLogStrategy(db, TopicConfig.CLEANUP_POLICY_DELETE, sourceTopics, queries)
    }


    @Test
    fun testCreateNodeWithConstraints() {
        kafkaConsumer.subscribe(listOf("personConstraints"))
        db.execute("CREATE (:PersonConstr {name:'Andrea'})")
        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertEquals(true, records.all {
            JSONUtils.asStreamsTransactionEvent(it.value()).let {
                val payload = it.payload as NodePayload
                val labels = payload.after!!.labels!!
                val properties = payload.after!!.properties
                labels == listOf("PersonConstr") && properties == mapOf("name" to "Andrea")
                        && it.meta.operation == OperationType.created
                        && it.schema.properties == mapOf("name" to "String")
                        && it.schema.constraints == listOf(Constraint("PersonConstr", setOf("name"), StreamsConstraintType.UNIQUE))
            }
        })
    }

    @Test
    fun testCreateRelationshipWithConstraints() {
        db.execute("CREATE (:PersonConstr {name:'Andrea'})")
        db.execute("CREATE (:ProductConstr {name:'My Awesome Product', price: '100€'})")
        db.execute("""
            |MATCH (p:PersonConstr {name:'Andrea'})
            |MATCH (pp:ProductConstr {name:'My Awesome Product'})
            |MERGE (p)-[:BOUGHT]->(pp)
        """.trimMargin())
        kafkaConsumer.subscribe(listOf("personConstraints", "productConstraints", "boughtConstraints"))
        val records = kafkaConsumer.poll(10000)
        assertEquals(3, records.count())

        val map = records
                .map {
                    val evt = JSONUtils.asStreamsTransactionEvent(it.value())
                    evt.payload.type to evt
                }
                .groupBy({ it.first }, { it.second })
        assertEquals(true, map[EntityType.node].orEmpty().isNotEmpty() && map[EntityType.node].orEmpty().all {
            val payload = it.payload as NodePayload
            val (labels, properties) = payload.after!!.labels!! to payload.after!!.properties!!
            when (labels) {
                listOf("ProductConstr") -> properties == mapOf("name" to "My Awesome Product", "price" to "100€")
                        && it.meta.operation == OperationType.created
                        && it.schema.properties == mapOf("name" to "String", "price" to "String")
                        && it.schema.constraints == listOf(Constraint("ProductConstr", setOf("name"), StreamsConstraintType.UNIQUE))
                listOf("PersonConstr") -> properties == mapOf("name" to "Andrea")
                        && it.meta.operation == OperationType.created
                        && it.schema.properties == mapOf("name" to "String")
                        && it.schema.constraints == listOf(Constraint("PersonConstr", setOf("name"), StreamsConstraintType.UNIQUE))
                else -> false
            }
        })
        assertEquals(true, map[EntityType.relationship].orEmpty().isNotEmpty() && map[EntityType.relationship].orEmpty().all {
            val payload = it.payload as RelationshipPayload
            val (start, end, properties) = Triple(payload.start, payload.end, payload.after!!.properties!!)
            properties.isNullOrEmpty()
                    && start.ids == mapOf("name" to "Andrea")
                    && end.ids == mapOf("name" to "My Awesome Product")
                    && it.meta.operation == OperationType.created
                    && it.schema.properties == emptyMap<String, String>()
                    && it.schema.constraints.toSet() == setOf(
                    Constraint("PersonConstr", setOf("name"), StreamsConstraintType.UNIQUE),
                    Constraint("ProductConstr", setOf("name"), StreamsConstraintType.UNIQUE))
        })
    }
}