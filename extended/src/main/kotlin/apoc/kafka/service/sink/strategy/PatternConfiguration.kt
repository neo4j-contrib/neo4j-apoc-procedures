package apoc.kafka.service.sink.strategy

import apoc.kafka.extensions.quote

enum class PatternConfigurationType { ALL, INCLUDE, EXCLUDE }

private const val ID_PREFIX = "!"
private const val MINUS_PREFIX = "-"
private const val LABEL_SEPARATOR = ":"
private const val PROPERTIES_SEPARATOR = ","

private fun getPatternConfiguredType(properties: List<String>): PatternConfigurationType {
    if (properties.isEmpty()) {
        return PatternConfigurationType.ALL
    }
    return when (properties[0].trim()[0]) {
        '*' -> PatternConfigurationType.ALL
        '-' -> PatternConfigurationType.EXCLUDE
        else -> PatternConfigurationType.INCLUDE
    }
}

private fun isHomogeneousPattern(type: PatternConfigurationType, properties: List<String>, pattern: String, entityType: String) {
    val isHomogeneous = when (type) {
        PatternConfigurationType.INCLUDE -> properties.all { it.trim()[0].isJavaIdentifierStart() }
        PatternConfigurationType.EXCLUDE -> properties.all { it.trim().startsWith(MINUS_PREFIX) }
        PatternConfigurationType.ALL -> properties.isEmpty() || properties == listOf("*")
    }
    if (!isHomogeneous) {
        throw IllegalArgumentException("The $entityType pattern $pattern is not homogeneous")
    }
}

private fun cleanProperties(type: PatternConfigurationType, properties: List<String>): List<String> {
    return when (type) {
        PatternConfigurationType.INCLUDE -> properties.map { it.trim() }
        PatternConfigurationType.EXCLUDE -> properties.map { it.trim().replace(MINUS_PREFIX, "") }
        PatternConfigurationType.ALL -> emptyList()
    }
}

interface PatternConfiguration

data class NodePatternConfiguration(val keys: Set<String>, val type: PatternConfigurationType,
                                    val labels: List<String>, val properties: List<String>): PatternConfiguration {
    companion object {

        // (:LabelA{!id,foo,bar})
        @JvmStatic private val cypherNodePatternConfigured = """\((:\w+\s*(?::\s*(?:\w+)\s*)*)\s*(?:\{\s*(-?[\w!\.]+\s*(?:,\s*-?[!\w\*\.]+\s*)*)\})?\)$""".toRegex()
        // LabelA{!id,foo,bar}
        @JvmStatic private val simpleNodePatternConfigured = """^(\w+\s*(?::\s*(?:\w+)\s*)*)\s*(?:\{\s*(-?[\w!\.]+\s*(?:,\s*-?[!\w\*\.]+\s*)*)\})?$""".toRegex()
        fun parse(pattern: String): NodePatternConfiguration {
            val isCypherPattern = pattern.startsWith("(")
            val regex = if (isCypherPattern) cypherNodePatternConfigured else simpleNodePatternConfigured
            val matcher = regex.matchEntire(pattern)
            if (matcher == null) {
                throw IllegalArgumentException("The Node pattern $pattern is invalid")
            } else {
                val labels = matcher.groupValues[1]
                        .split(LABEL_SEPARATOR)
                        .let {
                            if (isCypherPattern) it.drop(1) else it
                        }
                        .map { it.quote() }
                val allProperties = matcher.groupValues[2].split(PROPERTIES_SEPARATOR)
                val keys = allProperties
                        .filter { it.startsWith(ID_PREFIX) }
                        .map { it.trim().substring(1) }.toSet()
                if (keys.isEmpty()) {
                    throw IllegalArgumentException("The Node pattern $pattern must contains at lest one key")
                }
                val properties = allProperties.filter { !it.startsWith(ID_PREFIX) }
                val type = getPatternConfiguredType(properties)
                isHomogeneousPattern(type, properties, pattern, "Node")
                val cleanedProperties = cleanProperties(type, properties)

                return NodePatternConfiguration(keys = keys, type = type,
                        labels = labels, properties = cleanedProperties)
            }
        }
    }
}


data class RelationshipPatternConfiguration(val start: NodePatternConfiguration, val end: NodePatternConfiguration,
                                            val relType: String, val type: PatternConfigurationType,
                                            val properties: List<String>): PatternConfiguration {
    companion object {

        // we don't allow ALL for start/end nodes in rels
        // it's public for testing purpose
        fun getNodeConf(pattern: String): NodePatternConfiguration {
            val start = NodePatternConfiguration.parse(pattern)
            return if (start.type == PatternConfigurationType.ALL) {
                NodePatternConfiguration(keys = start.keys, type = PatternConfigurationType.INCLUDE,
                        labels = start.labels, properties = start.properties)
            } else {
                start
            }
        }

        // (:Source{!id})-[:REL_TYPE{foo, -bar}]->(:Target{!targetId})
        private val cypherRelationshipPatternConfigured = """^\(:(.*?)\)(<)?-\[(?::)([\w\_]+)(\{\s*(-?[\w\*\.]+\s*(?:,\s*-?[\w\*\.]+\s*)*)\})?\]-(>)?\(:(.*?)\)$""".toRegex()
        // LabelA{!id} REL_TYPE{foo, -bar} LabelB{!targetId}
        private val simpleRelationshipPatternConfigured =  """^(.*?) ([\w\_]+)(\{\s*(-?[\w\*\.]+\s*(?:,\s*-?[\w\*\.]+\s*)*)\})? (.*?)$""".toRegex() // """^\((.*?)\)-\[(?::)([\w\_]+)(\{\s*(-?[\w\*\.]+\s*(?:,\s*-?[\w\*\.]+\s*)*)\})?\]->\((.*?)\)$""".toRegex()

        data class RelationshipPatternMetaData(val startPattern: String, val endPattern: String, val relType: String, val properties: List<String>) {
            companion object {

                private fun toProperties(propGroup: String): List<String> = if (propGroup.isNullOrBlank()) {
                    emptyList()
                } else {
                    propGroup.split(PROPERTIES_SEPARATOR)
                }

                fun create(isCypherPattern: Boolean, isLeftToRight: Boolean, groupValues: List<String>): RelationshipPatternMetaData {
                    lateinit var start: String
                    lateinit var end: String
                    lateinit var relType: String
                    lateinit var props: List<String>

                    if (isCypherPattern) {
                        if (isLeftToRight) {
                            start = groupValues[1]
                            end = groupValues[7]
                        } else {
                            start = groupValues[7]
                            end = groupValues[1]
                        }
                        relType = groupValues[3]
                        props = toProperties(groupValues[5])
                    } else {
                        if (isLeftToRight) {
                            start = groupValues[1]
                            end = groupValues[5]
                        } else {
                            start = groupValues[5]
                            end = groupValues[1]
                        }
                        relType = groupValues[2]
                        props = toProperties(groupValues[4])
                    }

                    return RelationshipPatternMetaData(startPattern = start,
                            endPattern = end, relType = relType,
                            properties = props)
                }
            }
        }

        fun parse(pattern: String): RelationshipPatternConfiguration {
            val isCypherPattern = pattern.startsWith("(")
            val regex = if (isCypherPattern) {
                cypherRelationshipPatternConfigured
            } else {
                simpleRelationshipPatternConfigured
            }
            val matcher = regex.matchEntire(pattern)
            if (matcher == null) {
                throw IllegalArgumentException("The Relationship pattern $pattern is invalid")
            } else {
                val isLeftToRight = (!isCypherPattern || isUndirected(matcher) || isDirectedToRight(matcher))
                val isRightToLeft = if (isCypherPattern) isDirectedToLeft(matcher) else false

                if (!isLeftToRight && !isRightToLeft) {
                    throw IllegalArgumentException("The Relationship pattern $pattern has an invalid direction")
                }

                val metadata = RelationshipPatternMetaData.create(isCypherPattern, isLeftToRight, matcher.groupValues)

                val start = try {
                    getNodeConf(metadata.startPattern)
                } catch (e: Exception) {
                    throw IllegalArgumentException("The Relationship pattern $pattern is invalid")
                }
                val end = try {
                    getNodeConf(metadata.endPattern)
                } catch (e: Exception) {
                    throw IllegalArgumentException("The Relationship pattern $pattern is invalid")
                }
                val type = getPatternConfiguredType(metadata.properties)
                isHomogeneousPattern(type, metadata.properties, pattern, "Relationship")
                val cleanedProperties = cleanProperties(type, metadata.properties)
                return RelationshipPatternConfiguration(start = start, end = end, relType = metadata.relType,
                        properties = cleanedProperties, type = type)
            }
        }

        private fun isDirectedToLeft(matcher: MatchResult) =
                (matcher.groupValues[2] == "<" && matcher.groupValues[6] == "")

        private fun isDirectedToRight(matcher: MatchResult) =
                (matcher.groupValues[2] == "" && matcher.groupValues[6] == ">")

        private fun isUndirected(matcher: MatchResult) =
                (matcher.groupValues[2] == "" && matcher.groupValues[6] == "")
    }
}