package apoc.kafka.producer

import org.apache.commons.lang3.StringUtils
import org.neo4j.logging.Log
import apoc.kafka.config.StreamsConfig
import apoc.kafka.events.EntityType
import apoc.kafka.events.RelKeyStrategy


private inline fun <reified T> filterMap(config: Map<String, String>, routingPrefix: String, dbName: String = "", routingSuffix: String? = null, log: Log? = null): List<T> {
    val entityType = when (T::class) {
        NodeRoutingConfiguration::class -> EntityType.node
        RelationshipRoutingConfiguration::class -> EntityType.relationship
        else -> throw IllegalArgumentException("The class must be an instance of RoutingConfiguration")
    }
    return config
            .filterKeys {
                val startWithPrefixAndNotEndWithSuffix = it.startsWith(routingPrefix) && routingSuffix?.let { suffix -> !it.endsWith(suffix) } ?: true
                if (it.contains(StreamsRoutingConfigurationConstants.FROM)) {
                    val topicDbName = it.replace(routingPrefix, StringUtils.EMPTY)
                            .split(StreamsRoutingConfigurationConstants.FROM)[1]
                    startWithPrefixAndNotEndWithSuffix && topicDbName == dbName // for `from.<db>` we compare the routing prefix and the db name
                } else {
                    // for the default db we only filter by routingPrefix
                    dbName == "" && startWithPrefixAndNotEndWithSuffix
                }
            }
            .flatMap {
                val prefixAndTopic = it.key.split(StreamsRoutingConfigurationConstants.FROM)[0]

                val keyStrategy = routingSuffix?.let { suffix ->
                    print("suffix - $suffix")
                    config.entries.firstOrNull{ it.key.startsWith(prefixAndTopic) && it.key.endsWith(suffix) }?.value
                } ?: RelKeyStrategy.DEFAULT.toString().toLowerCase()

                RoutingConfigurationFactory
                    .getRoutingConfiguration(prefixAndTopic.replace(routingPrefix, StringUtils.EMPTY),
                            it.value, entityType, keyStrategy, log) as List<T>
            }
}

private object StreamsRoutingConfigurationConstants {
    const val NODE_ROUTING_KEY_PREFIX: String = "apoc.kafka.source.topic.nodes."
    const val REL_ROUTING_KEY_PREFIX: String = "apoc.kafka.source.topic.relationships."
    const val SCHEMA_POLLING_INTERVAL = "apoc.kafka.source.schema.polling.interval"
    const val FROM = ".from."
    const val KEY_STRATEGY_SUFFIX = ".key_strategy"
}

data class StreamsEventRouterConfiguration(val enabled: Boolean = StreamsConfig.SOURCE_ENABLED_VALUE,
                                           val proceduresEnabled: Boolean = StreamsConfig.PROCEDURES_ENABLED_VALUE,
                                           val nodeRouting: List<NodeRoutingConfiguration> = listOf(
                                               NodeRoutingConfiguration()
                                           ),
                                           val relRouting: List<RelationshipRoutingConfiguration> = listOf(
                                               RelationshipRoutingConfiguration()
                                           ),
                                           val schemaPollingInterval: Long = 300000) {

    fun allTopics(): List<String> {
        val nodeTopics = nodeRouting.map { it.topic }
        val relTopics = relRouting.map { it.topic }
        return nodeTopics + relTopics
    }

    companion object {

        fun from(streamsConfig: Map<String, String>, dbName: String, isDefaultDb: Boolean, log: Log? = null): StreamsEventRouterConfiguration {
            var nodeRouting = filterMap<NodeRoutingConfiguration>(config = streamsConfig,
                    routingPrefix = StreamsRoutingConfigurationConstants.NODE_ROUTING_KEY_PREFIX,
                    dbName = dbName)
            var relRouting = filterMap<RelationshipRoutingConfiguration>(config = streamsConfig,
                    routingPrefix = StreamsRoutingConfigurationConstants.REL_ROUTING_KEY_PREFIX,
                    dbName = dbName,
                    routingSuffix = StreamsRoutingConfigurationConstants.KEY_STRATEGY_SUFFIX,
                    log = log)

            if (isDefaultDb) {
                nodeRouting += filterMap(config = streamsConfig,
                        routingPrefix = StreamsRoutingConfigurationConstants.NODE_ROUTING_KEY_PREFIX
                )
                relRouting += filterMap(config = streamsConfig,
                        routingPrefix = StreamsRoutingConfigurationConstants.REL_ROUTING_KEY_PREFIX,
                        routingSuffix = StreamsRoutingConfigurationConstants.KEY_STRATEGY_SUFFIX,
                        log = log)
            }

            val default = StreamsEventRouterConfiguration()
            return default.copy(
                    enabled = StreamsConfig.isSourceEnabled(streamsConfig, dbName),
                    proceduresEnabled = StreamsConfig.hasProceduresEnabled(streamsConfig, dbName),
                    nodeRouting = if (nodeRouting.isEmpty()) listOf(NodeRoutingConfiguration(topic = dbName)) else nodeRouting,
                    relRouting = if (relRouting.isEmpty()) listOf(RelationshipRoutingConfiguration(topic = dbName)) else relRouting,
                    schemaPollingInterval = streamsConfig.getOrDefault(StreamsRoutingConfigurationConstants.SCHEMA_POLLING_INTERVAL, default.schemaPollingInterval).toString().toLong()
            )
        }

    }
}
