package apoc.kafka.consumer.kafka

//import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
//import apoc.kafka.consumer.StreamsSinkConfiguration
import apoc.kafka.extensions.toPointCase
import apoc.kafka.utils.JSONUtils
import apoc.kafka.utils.KafkaUtil.getInvalidTopics
import apoc.kafka.utils.KafkaUtil.validateConnection
import java.util.Properties


private const val kafkaConfigPrefix = "apoc.kafka."

//private val SUPPORTED_DESERIALIZER = listOf(ByteArrayDeserializer::class.java.name, KafkaAvroDeserializer::class.java.name)

private fun validateDeserializers(config: KafkaSinkConfiguration) {
//    val key = if (!SUPPORTED_DESERIALIZER.contains(config.keyDeserializer)) {
//        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
//    } else if (!SUPPORTED_DESERIALIZER.contains(config.valueDeserializer)) {
//        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
//    } else {
//        ""
//    }
//    if (key.isNotBlank()) {
//        throw RuntimeException("The property `kafka.$key` contains an invalid deserializer. Supported deserializers are $SUPPORTED_DESERIALIZER")
//    }
}

data class KafkaSinkConfiguration(val bootstrapServers: String = "localhost:9092",
                                  val keyDeserializer: String = "org.apache.kafka.common.serialization.ByteArrayDeserializer",
                                  val valueDeserializer: String = "org.apache.kafka.common.serialization.ByteArrayDeserializer",
                                  val groupId: String = "neo4j",
                                  val autoOffsetReset: String = "earliest",
//                                  val sinkConfiguration: StreamsSinkConfiguration = StreamsSinkConfiguration(),
                                  val enableAutoCommit: Boolean = true,
                                  val asyncCommit: Boolean = false,
                                  val extraProperties: Map<String, String> = emptyMap()) {

    companion object {

        fun from(cfg: Map<String, String>, dbName: String, isDefaultDb: Boolean): KafkaSinkConfiguration {
            val kafkaCfg = create(cfg, dbName, isDefaultDb)
            validate(kafkaCfg)
//            val invalidTopics = getInvalidTopics(kafkaCfg.asProperties(), kafkaCfg.sinkConfiguration.topics.allTopics())
//            return if (invalidTopics.isNotEmpty()) {
//                kafkaCfg.copy(sinkConfiguration = StreamsSinkConfiguration.from(cfg, dbName, invalidTopics, isDefaultDb))
//            } else {
                return kafkaCfg
//            }
        }

        // Visible for testing
        fun create(cfg: Map<String, String>, dbName: String, isDefaultDb: Boolean): KafkaSinkConfiguration {
            val config = cfg
                    .filterKeys { it.startsWith(kafkaConfigPrefix) && !it.startsWith("${kafkaConfigPrefix}sink") }
                    .mapKeys { it.key.substring(kafkaConfigPrefix.length) }
            val default = KafkaSinkConfiguration()

            val keys = JSONUtils.asMap(default).keys.map { it.toPointCase() }
            val extraProperties = config.filterKeys { !keys.contains(it) }

//            val streamsSinkConfiguration = StreamsSinkConfiguration.from(configMap = cfg, dbName = dbName, isDefaultDb = isDefaultDb)


            return default.copy(keyDeserializer = config.getOrDefault(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, default.keyDeserializer),
                    valueDeserializer = config.getOrDefault(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, default.valueDeserializer),
                    bootstrapServers = config.getOrDefault(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, default.bootstrapServers),
                    autoOffsetReset = config.getOrDefault(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, default.autoOffsetReset),
                    groupId = config.getOrDefault(ConsumerConfig.GROUP_ID_CONFIG, default.groupId) + (if (isDefaultDb) "" else "-$dbName"),
                    enableAutoCommit = config.getOrDefault(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, default.enableAutoCommit).toString().toBoolean(),
                    asyncCommit = config.getOrDefault("async.commit", default.asyncCommit).toString().toBoolean(),
//                    sinkConfiguration = streamsSinkConfiguration,
                    extraProperties = extraProperties // for what we don't provide a default configuration
            )
        }

        private fun validate(config: KafkaSinkConfiguration) {
            validateConnection(config.bootstrapServers, CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, false)
            val schemaRegistryUrlKey = "schema.registry.url"
            if (config.extraProperties.containsKey(schemaRegistryUrlKey)) {
                val schemaRegistryUrl = config.extraProperties.getOrDefault(schemaRegistryUrlKey, "")
                validateConnection(schemaRegistryUrl, schemaRegistryUrlKey, false)
            }
            validateDeserializers(config)
        }
    }

    fun asProperties(): Properties {
        val props = Properties()
        val map = JSONUtils.asMap(this)
                .filterKeys { it != "extraProperties" && it != "sinkConfiguration" }
                .mapKeys { it.key.toPointCase() }
        props.putAll(map)
        props.putAll(extraProperties)
        return props
    }
}