package apoc.kafka.extensions

import org.apache.avro.Schema
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.IndexedRecord
import org.neo4j.graphdb.Node
import apoc.kafka.utils.JSONUtils
import java.nio.ByteBuffer
import javax.lang.model.SourceVersion

fun Map<String,String>.getInt(name:String, defaultValue: Int) = this.get(name)?.toInt() ?: defaultValue


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

