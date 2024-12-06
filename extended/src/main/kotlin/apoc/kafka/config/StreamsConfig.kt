package apoc.kafka.config

import apoc.ApocConfig
import org.apache.commons.configuration2.ConfigurationMap
import org.apache.kafka.clients.consumer.ConsumerConfig

class StreamsConfig {

    companion object {

        fun getConfiguration(additionalConfigs: Map<String, String> = emptyMap()): Map<String, String> {
            val config = ApocConfig.apocConfig().config

            val map = ConfigurationMap(config)
                .filter { it.value is String }
                .toMutableMap() as Map<String, String>
            return convert(map, additionalConfigs)
        }
        
        const val SOURCE_ENABLED = "apoc.kafka.source.enabled"
        const val SOURCE_ENABLED_VALUE = true
        const val PROCEDURES_ENABLED = "apoc.kafka.procedures.enabled"
        const val PROCEDURES_ENABLED_VALUE = true

        fun isSourceGloballyEnabled(config: Map<String, Any?>) = config.getOrDefault(SOURCE_ENABLED, SOURCE_ENABLED_VALUE).toString().toBoolean()

        fun isSourceEnabled(config: Map<String, Any?>, dbName: String) = config.getOrDefault("${SOURCE_ENABLED}.from.$dbName", isSourceGloballyEnabled(config)).toString().toBoolean()

        fun hasProceduresGloballyEnabled(config: Map<String, Any?>) = config.getOrDefault(PROCEDURES_ENABLED, PROCEDURES_ENABLED_VALUE).toString().toBoolean()

        fun hasProceduresEnabled(config: Map<String, Any?>, dbName: String) = config.getOrDefault("${PROCEDURES_ENABLED}.$dbName", hasProceduresGloballyEnabled(config)).toString().toBoolean()

        fun convert(props: Map<String,String>, config: Map<String, String>): Map<String, String> {
            val mutProps = props.toMutableMap()
            val mappingKeys = mapOf(
                "broker" to "apoc.kafka.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}",
                "from" to "apoc.kafka.${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG}",
                "autoCommit" to "apoc.kafka.${ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG}",
                "keyDeserializer" to "apoc.kafka.${ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG}",
                "valueDeserializer" to "apoc.kafka.${ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG}",
                "schemaRegistryUrl" to "apoc.kafka.schema.registry.url",
                "groupId" to "apoc.kafka.${ConsumerConfig.GROUP_ID_CONFIG}")
            mutProps += config.mapKeys { mappingKeys.getOrDefault(it.key, it.key) }
            return mutProps
        }
    }
}