package apoc.kafka.common.support

import apoc.kafka.PublishProcedures
import apoc.kafka.consumer.procedures.StreamsSinkProcedures
import apoc.util.TestUtil
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.api.procedure.GlobalProcedures
import java.util.*

object KafkaTestUtils {
    fun <K, V> createConsumer(bootstrapServers: String,
                              schemaRegistryUrl: String? = null,
                              keyDeserializer: String = StringDeserializer::class.java.name,
                              valueDeserializer: String = ByteArrayDeserializer::class.java.name,
                              vararg topics: String = emptyArray()): KafkaConsumer<K, V> {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props["group.id"] = "neo4j" // UUID.randomUUID().toString()
        props["enable.auto.commit"] = "true"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = keyDeserializer
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = valueDeserializer
        props["auto.offset.reset"] = "earliest"
        if (schemaRegistryUrl != null) {
            props["schema.registry.url"] = schemaRegistryUrl
        }
        val consumer = KafkaConsumer<K, V>(props)
        if (!topics.isNullOrEmpty()) {
            consumer.subscribe(topics.toList())
        }
        return consumer
    }

    fun <K, V> createProducer(bootstrapServers: String,
                              schemaRegistryUrl: String? = null,
                              keySerializer: String = StringSerializer::class.java.name,
                              valueSerializer: String = ByteArraySerializer::class.java.name): KafkaProducer<K, V> {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = keySerializer
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = valueSerializer
        if (!schemaRegistryUrl.isNullOrBlank()) {
            props["schema.registry.url"] = schemaRegistryUrl
        }
        return KafkaProducer(props)
    }

    fun getDbServices(dbms: DatabaseManagementService): GraphDatabaseService {
        val db = dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)
        TestUtil.registerProcedure(db, StreamsSinkProcedures::class.java, GlobalProcedures::class.java, PublishProcedures::class.java);
        return db
    }
}