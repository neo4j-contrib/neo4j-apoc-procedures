package apoc.kafka.service

import apoc.kafka.service.sink.strategy.*
import kotlin.reflect.jvm.javaType

class TopicValidationException(message: String): RuntimeException(message)

private fun TopicType.replaceKeyBy(replacePrefix: Pair<String, String>) = if (replacePrefix.first.isNullOrBlank())
        this.key
    else
        this.key.replace(replacePrefix.first, replacePrefix.second)

data class Topics(val cypherTopics: Map<String, String> = emptyMap(),
                  val cdcSourceIdTopics: Set<String> = emptySet(),
                  val cdcSchemaTopics: Set<String> = emptySet(),
                  val cudTopics: Set<String> = emptySet(),
                  val nodePatternTopics: Map<String, NodePatternConfiguration> = emptyMap(),
                  val relPatternTopics: Map<String, RelationshipPatternConfiguration> = emptyMap(),
                  val invalid: List<String> = emptyList()) {

    operator fun plus(other: Topics): Topics {
        return Topics(cypherTopics = this.cypherTopics + other.cypherTopics,
                cdcSourceIdTopics = this.cdcSourceIdTopics + other.cdcSourceIdTopics,
                cdcSchemaTopics = this.cdcSchemaTopics + other.cdcSchemaTopics,
                cudTopics = this.cudTopics + other.cudTopics,
                nodePatternTopics = this.nodePatternTopics + other.nodePatternTopics,
                relPatternTopics = this.relPatternTopics + other.relPatternTopics,
                invalid = this.invalid + other.invalid)
    }

    fun allTopics(): List<String> = this.asMap()
            .map {
                if (it.key.group == TopicTypeGroup.CDC || it.key.group == TopicTypeGroup.CUD) {
                    (it.value as Set<String>).toList()
                } else {
                    (it.value as Map<String, Any>).keys.toList()
                }
            }
            .flatten()

    fun asMap(): Map<TopicType, Any> = mapOf(TopicType.CYPHER to cypherTopics, TopicType.CUD to cudTopics,
            TopicType.CDC_SCHEMA to cdcSchemaTopics, TopicType.CDC_SOURCE_ID to cdcSourceIdTopics,
            TopicType.PATTERN_NODE to nodePatternTopics, TopicType.PATTERN_RELATIONSHIP to relPatternTopics)

    companion object {
        fun from(map: Map<String, Any?>, replacePrefix: Pair<String, String> = ("" to ""), dbName: String = "", invalidTopics: List<String> = emptyList()): Topics {
            val config = map
                    .filterKeys { if (dbName.isNotBlank()) it.toLowerCase().endsWith(".to.$dbName") else !it.contains(".to.") }
                    .mapKeys { if (dbName.isNotBlank()) it.key.replace(".to.$dbName", "", true) else it.key }
            val cypherTopicPrefix = TopicType.CYPHER.replaceKeyBy(replacePrefix)
            val sourceIdKey = TopicType.CDC_SOURCE_ID.replaceKeyBy(replacePrefix)
            val schemaKey = TopicType.CDC_SCHEMA.replaceKeyBy(replacePrefix)
            val cudKey = TopicType.CUD.replaceKeyBy(replacePrefix)
            val nodePatterKey = TopicType.PATTERN_NODE.replaceKeyBy(replacePrefix)
            val relPatterKey = TopicType.PATTERN_RELATIONSHIP.replaceKeyBy(replacePrefix)
            val cypherTopics = TopicUtils.filterByPrefix(config, cypherTopicPrefix)
            val nodePatternTopics = TopicUtils
                    .filterByPrefix(config, nodePatterKey, invalidTopics)
                    .mapValues { NodePatternConfiguration.parse(it.value) }
            val relPatternTopics = TopicUtils
                    .filterByPrefix(config, relPatterKey, invalidTopics)
                    .mapValues { RelationshipPatternConfiguration.parse(it.value) }
            val cdcSourceIdTopics = TopicUtils.splitTopics(config[sourceIdKey] as? String, invalidTopics)
            val cdcSchemaTopics = TopicUtils.splitTopics(config[schemaKey] as? String, invalidTopics)
            val cudTopics = TopicUtils.splitTopics(config[cudKey] as? String, invalidTopics)
            return Topics(cypherTopics, cdcSourceIdTopics, cdcSchemaTopics, cudTopics, nodePatternTopics, relPatternTopics)
        }
    }
}

object TopicUtils {

    @JvmStatic val TOPIC_SEPARATOR = ";"

    fun filterByPrefix(config: Map<*, *>, prefix: String, invalidTopics: List<String> = emptyList()): Map<String, String> {
        val fullPrefix = "$prefix."
        return config
                .filterKeys { it.toString().startsWith(fullPrefix) }
                .mapKeys { it.key.toString().replace(fullPrefix, "") }
                .filterKeys { !invalidTopics.contains(it) }
                .mapValues { it.value.toString() }
    }

    fun splitTopics(cdcMergeTopicsString: String?, invalidTopics: List<String> = emptyList()): Set<String> {
        return if (cdcMergeTopicsString.isNullOrBlank()) {
            emptySet()
        } else {
            cdcMergeTopicsString.split(TOPIC_SEPARATOR)
                    .filter { !invalidTopics.contains(it) }
                    .toSet()
        }
    }

    inline fun <reified T: Throwable> validate(topics: Topics) {
        val exceptionStringConstructor = T::class.constructors
                .first { it.parameters.size == 1 && it.parameters[0].type.javaType == String::class.java }!!
        val crossDefinedTopics = topics.allTopics()
                .groupBy({ it }, { 1 })
                .filterValues { it.sum() > 1 }
                .keys
        if (crossDefinedTopics.isNotEmpty()) {
            throw exceptionStringConstructor
                    .call("The following topics are cross defined: $crossDefinedTopics")
        }
    }

    fun toStrategyMap(topics: Topics, sourceIdStrategyConfig: SourceIdIngestionStrategyConfig): Map<TopicType, Any> {
        return topics.asMap()
                .filterKeys { it != TopicType.CYPHER }
                .mapValues { (type, config) ->
                    when (type) {
                        TopicType.CDC_SOURCE_ID -> SourceIdIngestionStrategy(sourceIdStrategyConfig)
                        TopicType.CDC_SCHEMA -> SchemaIngestionStrategy()
                        TopicType.CUD -> CUDIngestionStrategy()
                        TopicType.PATTERN_NODE -> {
                            val map = config as Map<String, NodePatternConfiguration>
                            map.mapValues { NodePatternIngestionStrategy(it.value) }
                        }
                        TopicType.PATTERN_RELATIONSHIP -> {
                            val map = config as Map<String, RelationshipPatternConfiguration>
                            map.mapValues { RelationshipPatternIngestionStrategy(it.value) }
                        }
                        else -> throw RuntimeException("Unsupported topic type $type")
                    }
                }
    }
}