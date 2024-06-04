package apoc.kafka.utils

import apoc.kafka.events.StreamsTransactionEvent
import apoc.kafka.events.StreamsTransactionNodeEvent
import apoc.kafka.events.StreamsTransactionRelationshipEvent
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.kotlin.convertValue
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.neo4j.driver.internal.value.PointValue
import org.neo4j.graphdb.spatial.Point
import org.neo4j.values.storable.CoordinateReferenceSystem
import java.io.IOException
import java.time.temporal.TemporalAccessor

abstract class StreamsPoint { abstract val crs: String }
data class StreamsPointCartesian(override val crs: String, val x: Double, val y: Double, val z: Double? = null): StreamsPoint()
data class StreamsPointWgs(override val crs: String, val latitude: Double, val longitude: Double, val height: Double? = null): StreamsPoint()

fun Point.toStreamsPoint(): StreamsPoint {
    val crsType = this.crs.type
    val coordinate = this.coordinates[0].coordinate
    return when (this.crs) {
        CoordinateReferenceSystem.CARTESIAN -> StreamsPointCartesian(crsType, coordinate[0], coordinate[1])
        CoordinateReferenceSystem.CARTESIAN_3D -> StreamsPointCartesian(crsType, coordinate[0], coordinate[1], coordinate[2])
        CoordinateReferenceSystem.WGS_84 -> StreamsPointWgs(crsType, coordinate[0], coordinate[1])
        CoordinateReferenceSystem.WGS_84_3D -> StreamsPointWgs(crsType, coordinate[0], coordinate[1], coordinate[2])
        else -> throw IllegalArgumentException("Point type $crsType not supported")
    }
}

fun PointValue.toStreamsPoint(): StreamsPoint {
    val point = this.asPoint()
    return when (val crsType = point.srid()) {
        CoordinateReferenceSystem.CARTESIAN.code -> StreamsPointCartesian(CoordinateReferenceSystem.CARTESIAN.name, point.x(), point.y())
        CoordinateReferenceSystem.CARTESIAN_3D.code -> StreamsPointCartesian(CoordinateReferenceSystem.CARTESIAN_3D.name, point.x(), point.y(), point.z())
        CoordinateReferenceSystem.WGS_84.code -> StreamsPointWgs(CoordinateReferenceSystem.WGS_84.name, point.x(), point.y())
        CoordinateReferenceSystem.WGS_84_3D.code -> StreamsPointWgs(CoordinateReferenceSystem.WGS_84_3D.name, point.x(), point.y(), point.z())
        else -> throw IllegalArgumentException("Point type $crsType not supported")
    }
}

class PointSerializer : JsonSerializer<Point>() {
    @Throws(IOException::class, JsonProcessingException::class)
    override fun serialize(value: Point?, jgen: JsonGenerator,
                           provider: SerializerProvider) {
        if (value == null) {
            return
        }
        jgen.writeObject(value.toStreamsPoint())
    }
}

class PointValueSerializer : JsonSerializer<PointValue>() {
    @Throws(IOException::class, JsonProcessingException::class)
    override fun serialize(value: PointValue?, jgen: JsonGenerator,
                           provider: SerializerProvider) {
        if (value == null) {
            return
        }
        jgen.writeObject(value.toStreamsPoint())
    }
}

class TemporalAccessorSerializer : JsonSerializer<TemporalAccessor>() {
    @Throws(IOException::class, JsonProcessingException::class)
    override fun serialize(value: TemporalAccessor?, jgen: JsonGenerator,
                           provider: SerializerProvider) {
        if (value == null) {
            return
        }
        jgen.writeString(value.toString())
    }
}

// NOTE: it works differently from apoc.JSONUtil
object JSONUtils {

    private val OBJECT_MAPPER: ObjectMapper = jacksonObjectMapper()
    private val STRICT_OBJECT_MAPPER: ObjectMapper = jacksonObjectMapper()

    init {
        val module = SimpleModule("Neo4jKafkaSerializer")
        KafkaUtil.ignoreExceptions({ module.addSerializer(Point::class.java, PointSerializer()) }, NoClassDefFoundError::class.java) // in case is loaded from
        KafkaUtil.ignoreExceptions({ module.addSerializer(PointValue::class.java, PointValueSerializer()) }, NoClassDefFoundError::class.java) // in case is loaded from
        module.addSerializer(TemporalAccessor::class.java, TemporalAccessorSerializer())
        OBJECT_MAPPER.registerModule(module)
        OBJECT_MAPPER.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
        OBJECT_MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        STRICT_OBJECT_MAPPER.registerModule(module)
    }

    fun getObjectMapper(): ObjectMapper = OBJECT_MAPPER

    fun getStrictObjectMapper(): ObjectMapper = STRICT_OBJECT_MAPPER

    fun asMap(any: Any): Map<String, Any?> {
        return OBJECT_MAPPER.convertValue(any, Map::class.java)
                .mapKeys { it.key.toString() }
    }

    fun writeValueAsString(any: Any): String {
        return OBJECT_MAPPER.writeValueAsString(any)
    }

    fun writeValueAsBytes(any: Any): ByteArray {
        return OBJECT_MAPPER.writeValueAsBytes(any)
    }

    fun <T> readValue(value: ByteArray, valueType: Class<T>?): T {
        return getObjectMapper().readValue(value, valueType)
    }

    fun readValue(value: ByteArray): Any {
        return getObjectMapper().readValue(value)
    }

    inline fun <reified T> convertValue(value: Any, objectMapper: ObjectMapper = getObjectMapper()): T {
        return objectMapper.convertValue(value)
    }

    fun asStreamsTransactionEvent(obj: Any): StreamsTransactionEvent {
        return try {
            val evt = when (obj) {
                is String, is ByteArray -> STRICT_OBJECT_MAPPER.readValue(obj as ByteArray, StreamsTransactionNodeEvent::class.java)
                else -> STRICT_OBJECT_MAPPER.convertValue(obj, StreamsTransactionNodeEvent::class.java)
            }
            evt.toStreamsTransactionEvent()
        } catch (e: Exception) {
            val evt = when (obj) {
                is String, is ByteArray -> STRICT_OBJECT_MAPPER.readValue(obj as ByteArray, StreamsTransactionRelationshipEvent::class.java)
                else -> STRICT_OBJECT_MAPPER.convertValue(obj, StreamsTransactionRelationshipEvent::class.java)
            }
            evt.toStreamsTransactionEvent()
        }
    }
}