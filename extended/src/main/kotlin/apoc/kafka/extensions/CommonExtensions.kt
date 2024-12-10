package apoc.kafka.extensions

import org.apache.avro.Schema
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.IndexedRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.neo4j.graphdb.Node
import apoc.kafka.utils.JSONUtils
import apoc.kafka.service.StreamsSinkEntity
import java.nio.ByteBuffer
import java.util.*
import javax.lang.model.SourceVersion

fun Map<String,String>.getInt(name:String, defaultValue: Int) = this.get(name)?.toInt() ?: defaultValue
fun Map<*, *>.asProperties() = this.let {
    val properties = Properties()
    properties.putAll(it)
    properties
}

fun Node.labelNames() : List<String> {
    return this.labels.map { it.name() }
}

fun String.toPointCase(): String {
    return this.split("(?<=[a-z])(?=[A-Z])".toRegex()).joinToString(separator = ".").toLowerCase()
}

fun String.quote(): String = if (SourceVersion.isIdentifier(this)) this else "`$this`"

fun Map<String, Any?>.flatten(map: Map<String, Any?> = this, prefix: String = ""): Map<String, Any?> {
    return map.flatMap {
        val key = it.key
        val value = it.value
        val newKey = if (prefix != "") "$prefix.$key" else key
        if (value is Map<*, *>) {
            flatten(value as Map<String, Any>, newKey).toList()
        } else {
            listOf(newKey to value)
        }
    }.toMap()
}

fun ConsumerRecord<*, *>.topicPartition() = TopicPartition(this.topic(), this.partition())
fun ConsumerRecord<*, *>.offsetAndMetadata(metadata: String = "") = OffsetAndMetadata(this.offset() + 1, metadata)

private fun convertAvroData(rawValue: Any?): Any? = when (rawValue) {
    is IndexedRecord -> rawValue.toMap()
    is Collection<*> -> rawValue.map(::convertAvroData)
    is Array<*> -> if (rawValue.javaClass.componentType.isPrimitive) rawValue else rawValue.map(::convertAvroData)
    is Map<*, *> -> rawValue
            .mapKeys { it.key.toString() }
            .mapValues { convertAvroData(it.value) }
    is GenericFixed -> rawValue.bytes()
    is ByteBuffer -> rawValue.array()
    is GenericEnumSymbol<*>, is CharSequence -> rawValue.toString()
    else -> rawValue
}
fun IndexedRecord.toMap() = this.schema.fields
        .map { it.name() to convertAvroData(this[it.pos()]) }
        .toMap()

fun Schema.toMap() = JSONUtils.asMap(this.toString())

private fun convertData(data: Any?, stringWhenFailure: Boolean = false): Any? {
    return when (data) {
        null -> null
        is ByteArray -> JSONUtils.readValue(data, Any::class.java)
        is GenericRecord -> data.toMap()
        else -> if (stringWhenFailure) data.toString() else throw RuntimeException("Unsupported type ${data::class.java.name}")
    }
}
fun ConsumerRecord<*, *>.toStreamsSinkEntity(): StreamsSinkEntity {
    val key = convertData(this.key(), true)
    val value = convertData(this.value())
    return StreamsSinkEntity(key, value)
}