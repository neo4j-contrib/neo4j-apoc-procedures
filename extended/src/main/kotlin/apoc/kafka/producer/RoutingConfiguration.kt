package apoc.kafka.producer

import org.apache.commons.lang3.StringUtils
import org.apache.kafka.common.internals.Topic
import org.neo4j.graphdb.Entity
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.logging.Log
import apoc.kafka.events.*


private val PATTERN_REG: Regex = "^(\\s*\\:*\\s*\\`*\\s*\\w+\\s*(?:\\:*\\s*\\`*\\s*\\:?(?:[\\w\\`|\\*]+)\\s*)*\\`*\\:?)\\s*(?:\\{\\s*(-?[\\w|\\*]+\\s*(?:,\\s*-?[\\w|\\*]+\\s*)*)\\})?\$".toRegex()
private val PATTERN_COLON_REG = "\\s*:\\s*(?=(?:[^\\`]*\\`[^\\`]*\\`)*[^\\`]*\$)".toRegex()
private val PATTERN_COMMA = "\\s*,\\s*".toRegex()
private const val PATTERN_WILDCARD = "*"
private const val PATTERN_PROP_MINUS = '-'
private const val PATTERN_SPLIT = ";"
private const val BACKTICK_CHAR = "`"

data class RoutingProperties(val all: Boolean,
                             val include: List<String>,
                             val exclude: List<String>) {
    companion object {
        fun from(matcher: MatchResult): RoutingProperties {
            val props = matcher.groupValues[2].trim().let { if (it.isEmpty()) emptyList() else it.trim().split(
                PATTERN_COMMA
            ) }
            val include = if (props.isEmpty()) {
                emptyList()
            } else {
                props.filter { it != PATTERN_WILDCARD && !it.startsWith(PATTERN_PROP_MINUS) }
            }
            val exclude = if (props.isEmpty()) {
                emptyList()
            } else {
                props.filter { it != PATTERN_WILDCARD && it.startsWith(PATTERN_PROP_MINUS) }.map { it.substring(1) }
            }
            val all = props.isEmpty() || props.contains(PATTERN_WILDCARD)
            return RoutingProperties(all = all, include = include, exclude = exclude)
        }
    }
}

abstract class RoutingConfiguration {
    abstract val topic: String
    abstract val all: Boolean
    abstract val include: List<String>
    abstract val exclude: List<String>
    abstract fun filter(entity: Entity): Map<String, Any?>
}

private fun hasLabel(label: String, streamsTransactionEvent: StreamsTransactionEvent): Boolean {
    if (streamsTransactionEvent.payload.type == EntityType.relationship) {
        return false
    }
    val payload = when(streamsTransactionEvent.meta.operation) {
        OperationType.deleted -> streamsTransactionEvent.payload.before as NodeChange
        else -> streamsTransactionEvent.payload.after as NodeChange
    }
    return payload.labels.orEmpty().contains(label)
}

private fun isRelationshipType(name: String, streamsTransactionEvent: StreamsTransactionEvent): Boolean {
    if (streamsTransactionEvent.payload.type == EntityType.node) {
        return false
    }
    val relationshipChange = streamsTransactionEvent.payload as RelationshipPayload
    return relationshipChange.label == name
}

private fun filterProperties(properties: Map<String, Any>?, routingConfiguration: RoutingConfiguration): Map<String, Any>? {
    if (properties == null) {
        return null
    }
    if (!routingConfiguration.all) {
        if (routingConfiguration.include.isNotEmpty()) {
            return properties!!.filter { prop -> routingConfiguration.include.contains(prop.key) }
        }
        if (routingConfiguration.exclude.isNotEmpty()) {
            return properties!!.filter { prop -> !routingConfiguration.exclude.contains(prop.key) }
        }

    }
    return properties
}

data class NodeRoutingConfiguration(val labels: List<String> = emptyList(),
                                    override val topic: String = "neo4j",
                                    override val all: Boolean = true,
                                    override val include: List<String> = emptyList(),
                                    override val exclude: List<String> = emptyList()): RoutingConfiguration() {

    override fun filter(node: Entity): Map<String, Any?> {
        if (node !is Node) {
            throw IllegalArgumentException("argument must be and instance of ${Node::class.java.name}")
        }
        val properties = filterProperties(node.allProperties, this)
        val map = node.toMap().toMutableMap()
        map["properties"] = properties
        return map
    }

    companion object {
        fun parse(topic: String, pattern: String): List<NodeRoutingConfiguration> {
            Topic.validate(topic)
            if (pattern == PATTERN_WILDCARD) {
                return listOf(NodeRoutingConfiguration(topic = topic))
            }
            return pattern.split(PATTERN_SPLIT).map {
                val matcher = PATTERN_REG.matchEntire(it)
                if (matcher == null) {
                    throw IllegalArgumentException("The pattern $pattern for topic $topic is invalid")
                } else {
                    val labels = matcher.groupValues[1].trim().split(PATTERN_COLON_REG).map { it.replace(BACKTICK_CHAR, StringUtils.EMPTY) }.filter{ it.isNotBlank() }
                    val properties = RoutingProperties.from(matcher)
                    NodeRoutingConfiguration(labels = labels, topic = topic, all = properties.all,
                            include = properties.include, exclude = properties.exclude)
                }
            }
        }

        fun prepareEvent(streamsTransactionEvent: StreamsTransactionEvent, routingConf: List<NodeRoutingConfiguration>): Map<String, StreamsTransactionEvent> {
            return routingConf
                    .filter {
                        it.labels.isEmpty() || it.labels.any { hasLabel(it, streamsTransactionEvent) }
                    }
                    .map {
                        val nodePayload = streamsTransactionEvent.payload as NodePayload
                        val newRecordBefore = if (nodePayload.before != null) {
                            val recordBefore = nodePayload.before as NodeChange
                            recordBefore.copy(properties = filterProperties(streamsTransactionEvent.payload.before?.properties, it),
                                    labels = recordBefore.labels)
                        } else {
                            null
                        }
                        val newRecordAfter = if (nodePayload.after != null) {
                            val recordAfter = nodePayload.after as NodeChange
                            recordAfter.copy(properties = filterProperties(streamsTransactionEvent.payload.after?.properties, it),
                                    labels = recordAfter.labels)
                        } else {
                            null
                        }

                        val newNodePayload = nodePayload.copy(id = nodePayload.id,
                                before = newRecordBefore,
                                after = newRecordAfter)

                        val newStreamsEvent = streamsTransactionEvent.copy(schema = streamsTransactionEvent.schema,
                                meta = streamsTransactionEvent.meta,
                                payload = newNodePayload)

                        it.topic to newStreamsEvent
                    }
                    .associateBy({ it.first }, { it.second })
        }
    }
}

data class RelationshipRoutingConfiguration(val name: String = "",
                                            val relKeyStrategy: RelKeyStrategy = RelKeyStrategy.DEFAULT,
                                            override val topic: String = "neo4j",
                                            override val all: Boolean = true,
                                            override val include: List<String> = emptyList(),
                                            override val exclude: List<String> = emptyList()): RoutingConfiguration() {

    override fun filter(relationship: Entity): Map<String, Any?> {
        if (relationship !is Relationship) {
            throw IllegalArgumentException("argument must be and instance of ${Relationship::class.java.name}")
        }
        val properties = filterProperties(relationship.allProperties, this)
        val map = relationship.toMap().toMutableMap()
        map["properties"] = properties
        return map
    }

    companion object {
        fun parse(topic: String, pattern: String, keyStrategyString: String = RelKeyStrategy.DEFAULT.toString(), log: Log? = null): List<RelationshipRoutingConfiguration> {
            Topic.validate(topic)
            if (pattern == PATTERN_WILDCARD) {
                return listOf(RelationshipRoutingConfiguration(topic = topic))
            }
            return pattern.split(PATTERN_SPLIT).map {
                val matcher = PATTERN_REG.matchEntire(it)
                if (matcher == null) {
                    throw IllegalArgumentException("The pattern $pattern for topic $topic is invalid")
                } else {
                    val labels = matcher.groupValues[1].split(PATTERN_COLON_REG)
                    if (labels.size > 1) {
                        throw IllegalArgumentException("The pattern $pattern for topic $topic is invalid")
                    }
                    val properties = RoutingProperties.from(matcher)

                    val relKeyStrategy = try {
                        RelKeyStrategy.valueOf(keyStrategyString.toUpperCase())
                    } catch (e: IllegalArgumentException) {
                        log?.warn("Invalid key strategy setting, switching to default value ${RelKeyStrategy.DEFAULT.toString().toLowerCase()}")
                        RelKeyStrategy.DEFAULT
                    }

                    RelationshipRoutingConfiguration(name = labels.first().trim().replace(BACKTICK_CHAR, StringUtils.EMPTY),
                            topic = topic, all = properties.all,
                            include = properties.include, exclude = properties.exclude, relKeyStrategy = relKeyStrategy)
                }
            }
        }

        fun prepareEvent(streamsTransactionEvent: StreamsTransactionEvent, routingConf: List<RelationshipRoutingConfiguration>): Map<String, StreamsTransactionEvent> {
            return routingConf
                    .filter {
                        it.name.isNullOrBlank() || isRelationshipType(it.name, streamsTransactionEvent)
                    }
                    .map {
                        val relationshipPayload = streamsTransactionEvent.payload as RelationshipPayload

                        val newRecordBefore = if (relationshipPayload.before != null) {
                            val recordBefore = relationshipPayload.before as RelationshipChange
                            recordBefore.copy(properties = filterProperties(streamsTransactionEvent.payload.before?.properties, it))
                        } else {
                            null
                        }
                        val newRecordAfter = if (relationshipPayload.after != null) {
                            val recordAfter = relationshipPayload.after as RelationshipChange
                            recordAfter.copy(properties = filterProperties(streamsTransactionEvent.payload.after?.properties, it))
                        } else {
                            null
                        }

                        val newRelationshipPayload = relationshipPayload.copy(id = relationshipPayload.id,
                                before = newRecordBefore,
                                after = newRecordAfter,
                                label = relationshipPayload.label)

                        val newStreamsEvent = streamsTransactionEvent.copy(schema = streamsTransactionEvent.schema,
                                meta = streamsTransactionEvent.meta,
                                payload = newRelationshipPayload)

                        it.topic to newStreamsEvent
                    }
                    .associateBy({ it.first }, { it.second })
        }
    }
}

object RoutingConfigurationFactory {
    fun getRoutingConfiguration(topic: String, line: String, entityType: EntityType, keyStrategy: String = RelKeyStrategy.DEFAULT.toString(), log: Log? = null): List<RoutingConfiguration> {
        return when (entityType) {
            EntityType.node -> NodeRoutingConfiguration.parse(topic, line)
            EntityType.relationship -> RelationshipRoutingConfiguration.parse(topic, line, keyStrategy, log)
        }
    }
}
