package apoc.kafka.producer

import apoc.kafka.events.EntityType
import apoc.kafka.events.NodeChange
import apoc.kafka.events.NodePayload
import apoc.kafka.events.OperationType
import apoc.kafka.events.RelationshipNodeChange
import apoc.kafka.events.RelationshipPayload
import apoc.kafka.events.Schema
import apoc.kafka.events.StreamsConstraintType
import apoc.kafka.events.StreamsTransactionEvent
import apoc.kafka.extensions.labelNames
import apoc.kafka.utils.KafkaUtil.getNodeKeys
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.config.TopicConfig
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.schema.ConstraintDefinition
import org.neo4j.graphdb.schema.ConstraintType

fun Node.toMap(): Map<String, Any?> {
    return mapOf("id" to id.toString(), "properties" to allProperties, "labels" to labelNames(), "type" to EntityType.node)
}

fun Relationship.toMap(): Map<String, Any?> {
    return mapOf("id" to id.toString(), "properties" to allProperties, "label" to type.name(),
            "start" to startNode.toMap(),
            "end" to endNode.toMap(),
            "type" to EntityType.relationship)
}

fun RecordMetadata.toMap(): Map<String, Any> = mapOf(
        "offset" to offset(),
        "timestamp" to timestamp(),
        "keySize" to serializedKeySize(),
        "valueSize" to serializedValueSize(),
        "partition" to partition()
)

fun ConstraintDefinition.streamsConstraintType(): StreamsConstraintType {
    return when (this.constraintType) {
        ConstraintType.UNIQUENESS, ConstraintType.NODE_KEY -> StreamsConstraintType.UNIQUE
        else -> if (isNodeConstraint()) StreamsConstraintType.NODE_PROPERTY_EXISTS else StreamsConstraintType.RELATIONSHIP_PROPERTY_EXISTS
    }
}

fun ConstraintDefinition.isNodeConstraint(): Boolean {
    return try { this.label; true } catch (e: IllegalStateException) { false }
}

fun ConstraintDefinition.isRelationshipConstraint(): Boolean {
    return try { this.relationshipType; true } catch (e: IllegalStateException) { false }
}

fun StreamsTransactionEvent.asSourceRecordValue(strategy: String): StreamsTransactionEvent? =
        if(isStrategyCompact(strategy) && meta.operation == OperationType.deleted) null else this

fun StreamsTransactionEvent.asSourceRecordKey(strategy: String): Any =
        when {
            isStrategyCompact(strategy) && payload is NodePayload -> nodePayloadAsMessageKey(payload as NodePayload, schema)
            isStrategyCompact(strategy) && payload is RelationshipPayload -> relationshipAsMessageKey(payload as RelationshipPayload)
            else -> "${meta.txId + meta.txEventId}-${meta.txEventId}"
        }

private fun nodePayloadAsMessageKey(payload: NodePayload, schema: Schema) = run {
    val nodeChange: NodeChange = payload.after ?: payload.before!!
    val labels = nodeChange.labels ?: emptyList()
    val props: Map<String, Any> = nodeChange.properties ?: emptyMap()
    val keys = getNodeKeys(labels, props.keys, schema.constraints)
    val ids = props.filterKeys { keys.contains(it) }

    if (ids.isEmpty()) payload.id else mapOf("ids" to ids, "labels" to labels)
}

private fun RelationshipNodeChange.toKey(): Any = if (ids.isEmpty()) id else mapOf("ids" to ids, "labels" to labels)

private fun relationshipAsMessageKey(payload: RelationshipPayload) = mapOf(
        "start" to payload.start.toKey(),
        "end" to payload.end.toKey(),
        "label" to payload.label)

private fun isStrategyCompact(strategy: String) = strategy == TopicConfig.CLEANUP_POLICY_COMPACT