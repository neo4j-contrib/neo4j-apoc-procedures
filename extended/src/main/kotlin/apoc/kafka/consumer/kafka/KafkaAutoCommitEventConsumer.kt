package apoc.kafka.consumer.kafka

//import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.neo4j.logging.Log
import apoc.kafka.consumer.StreamsEventConsumer
import apoc.kafka.extensions.offsetAndMetadata
import apoc.kafka.extensions.toStreamsSinkEntity
import apoc.kafka.service.StreamsSinkEntity
import apoc.kafka.service.errors.*
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean

data class KafkaTopicConfig(val commit: Boolean, val topicPartitionsMap: Map<TopicPartition, Long>) {
    companion object {
        private fun toTopicPartitionMap(topicConfig: Map<String,
                List<Map<String, Any>>>): Map<TopicPartition, Long> = topicConfig
                .flatMap { topicConfigEntry ->
                    topicConfigEntry.value.map {
                        val partition = it.getValue("partition").toString().toInt()
                        val offset = it.getValue("offset").toString().toLong()
                        TopicPartition(topicConfigEntry.key, partition) to offset
                    }
                }
                .toMap()

        fun fromMap(map: Map<String, Any>): KafkaTopicConfig {
            val commit = map.getOrDefault("commit", true).toString().toBoolean()
            val topicPartitionsMap = toTopicPartitionMap(map
                    .getOrDefault("partitions", emptyMap<String, List<Map<String, Any>>>()) as Map<String, List<Map<String, Any>>>)
            return KafkaTopicConfig(commit = commit, topicPartitionsMap = topicPartitionsMap)
        }
    }
}

abstract class KafkaEventConsumer(config: KafkaSinkConfiguration,
                                  log: Log,
                                  topics: Set<String>): StreamsEventConsumer(log, topics) {
    abstract fun wakeup()
}

open class KafkaAutoCommitEventConsumer(private val config: KafkaSinkConfiguration,
                                        private val log: Log,
                                        val topics: Set<String>,
                                        private val dbName: String): KafkaEventConsumer(config, log, topics) {

    private val errorService: ErrorService = KafkaErrorService(config.asProperties(),
        ErrorService.ErrorConfig.from(emptyMap()),
        { s, e -> log.error(s,e as Throwable) })

    // override fun invalidTopics(): List<String> = config.sinkConfiguration.topics.invalid

    private val isSeekSet = AtomicBoolean()

    val consumer: KafkaConsumer<*, *> = when {
        config.keyDeserializer == ByteArrayDeserializer::class.java.name && config.valueDeserializer == ByteArrayDeserializer::class.java.name -> KafkaConsumer<ByteArray, ByteArray>(config.asProperties())
//        config.keyDeserializer == ByteArrayDeserializer::class.java.name && config.valueDeserializer == KafkaAvroDeserializer::class.java.name -> KafkaConsumer<ByteArray, GenericRecord>(config.asProperties())
//        config.keyDeserializer == KafkaAvroDeserializer::class.java.name && config.valueDeserializer == KafkaAvroDeserializer::class.java.name -> KafkaConsumer<GenericRecord, GenericRecord>(config.asProperties())
//        config.keyDeserializer == KafkaAvroDeserializer::class.java.name && config.valueDeserializer == ByteArrayDeserializer::class.java.name -> KafkaConsumer<GenericRecord, ByteArray>(config.asProperties())
        else -> throw RuntimeException("Invalid config")
    }

    override fun start() {
        if (topics.isEmpty()) {
            log.info("No topics specified Kafka Consumer will not started")
            return
        }
        this.consumer.subscribe(topics)
    }

    override fun stop() {
        consumer.close()
        errorService.close()
    }

    private fun readSimple(action: (String, List<StreamsSinkEntity>) -> Unit) {
        val records = consumer.poll(Duration.ZERO)
        if (records.isEmpty) return
        this.topics.forEach { topic ->
            val topicRecords = records.records(topic)
            executeAction(action, topic, topicRecords)
        }
    }

    fun executeAction(action: (String, List<StreamsSinkEntity>) -> Unit, topic: String, topicRecords: Iterable<ConsumerRecord<out Any, out Any>>) {
        try {
            action(topic, topicRecords.map { it.toStreamsSinkEntity() })
        } catch (e: Exception) {
            errorService.report(topicRecords.map { ErrorData.from(it, e, this::class.java, dbName) })
        }
    }

    fun readFromPartition(kafkaTopicConfig: KafkaTopicConfig,
                          action: (String, List<StreamsSinkEntity>) -> Unit): Map<TopicPartition, OffsetAndMetadata> {
        setSeek(kafkaTopicConfig.topicPartitionsMap)
        val records = consumer.poll(Duration.ZERO)
        return when (records.isEmpty) {
            true -> emptyMap()
            else -> kafkaTopicConfig.topicPartitionsMap
                    .mapValues { records.records(it.key) }
                    .filterValues { it.isNotEmpty() }
                    .mapValues { (topic, topicRecords) ->
                        executeAction(action, topic.topic(), topicRecords)
                        topicRecords.last().offsetAndMetadata()
                    }
        }
    }

    override fun read(action: (String, List<StreamsSinkEntity>) -> Unit) {
        readSimple(action)
    }

    override fun read(topicConfig: Map<String, Any>, action: (String, List<StreamsSinkEntity>) -> Unit) {
        val kafkaTopicConfig = KafkaTopicConfig.fromMap(topicConfig)
        if (kafkaTopicConfig.topicPartitionsMap.isEmpty()) {
            readSimple(action)
        } else {
            readFromPartition(kafkaTopicConfig, action)
        }
    }

    private fun setSeek(topicPartitionsMap: Map<TopicPartition, Long>) {
        if (!isSeekSet.compareAndSet(false, true)) {
            return
        }
        consumer.poll(0) // dummy call see: https://stackoverflow.com/questions/41008610/kafkaconsumer-0-10-java-api-error-message-no-current-assignment-for-partition
        topicPartitionsMap.forEach {
            when (it.value) {
                -1L -> consumer.seekToBeginning(listOf(it.key))
                -2L -> consumer.seekToEnd(listOf(it.key))
                else -> consumer.seek(it.key, it.value)
            }
        }
    }

    override fun wakeup() {
        consumer.wakeup()
    }
}

