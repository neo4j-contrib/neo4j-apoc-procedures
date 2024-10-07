package apoc.kafka.events


enum class OperationType { created, updated, deleted }

data class Meta(val timestamp: Long,
                val username: String,
                val txId: Long,
                val txEventId: Int,
                val txEventsCount: Int,
                val operation: OperationType,
                val source: Map<String, Any> = emptyMap())


enum class EntityType { node, relationship }

data class RelationshipNodeChange(val id: String,
                                  val labels: List<String>?,
                                  val ids: Map<String, Any>)

abstract class RecordChange{ abstract val properties: Map<String, Any>? }
data class NodeChange(override val properties: Map<String, Any>?,
                      val labels: List<String>?): RecordChange()

data class RelationshipChange(override val properties: Map<String, Any>?): RecordChange()

abstract class Payload {
    abstract val id: String
    abstract val type: EntityType
    abstract val before: RecordChange?
    abstract val after: RecordChange?
}
data class NodePayload(override val id: String,
                       override val before: NodeChange?,
                       override val after: NodeChange?,
                       override val type: EntityType = EntityType.node): Payload()

data class RelationshipPayload(override val id: String,
                               val start: RelationshipNodeChange,
                               val end: RelationshipNodeChange,
                               override val before: RelationshipChange?,
                               override val after: RelationshipChange?,
                               val label: String,
                               override val type: EntityType = EntityType.relationship): Payload()

enum class StreamsConstraintType { UNIQUE, NODE_PROPERTY_EXISTS, RELATIONSHIP_PROPERTY_EXISTS }

enum class RelKeyStrategy { DEFAULT, ALL }

data class Constraint(val label: String?,
                      val properties: Set<String>,
                      val type: StreamsConstraintType)

data class Schema(val properties: Map<String, String> = emptyMap(),
                  val constraints: List<Constraint> = emptyList())

open class StreamsEvent(open val payload: Any)
data class StreamsTransactionEvent(val meta: Meta, override val payload: Payload, val schema: Schema): StreamsEvent(payload)

data class StreamsTransactionNodeEvent(val meta: Meta,
                                       val payload: NodePayload,
                                       val schema: Schema) {
    fun toStreamsTransactionEvent() = StreamsTransactionEvent(this.meta, this.payload, this.schema)
}
data class StreamsTransactionRelationshipEvent(val meta: Meta,
                                               val payload: RelationshipPayload,
                                               val schema: Schema) {
    fun toStreamsTransactionEvent() = StreamsTransactionEvent(this.meta, this.payload, this.schema)
}

