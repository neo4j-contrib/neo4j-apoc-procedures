package apoc.kafka.service.errors

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.record.RecordBatch
import apoc.kafka.extensions.toMap
import apoc.kafka.utils.JSONUtils
import java.util.*


data class ErrorData(val originalTopic: String,
                     val timestamp: Long,
                     val key: ByteArray?,
                     val value: ByteArray?,
                     val partition: String,
                     val offset: String,
                     val executingClass: Class<*>?,
                     val databaseName: String?,
                     val exception: Exception?) {

    constructor(originalTopic: String, timestamp: Long?, key: Any?, value: Any?,
                partition: Int, offset: Long, executingClass: Class<*>?, databaseName: String?, exception: Exception?) :
            this(originalTopic, timestamp ?: RecordBatch.NO_TIMESTAMP, toByteArray(key), toByteArray(value), partition.toString(),offset.toString(), executingClass, databaseName, exception)

    companion object {

        fun from(consumerRecord: ConsumerRecord<out Any, out Any>, exception: Exception?, executingClass: Class<*>?, databaseName: String?): ErrorData {
            return ErrorData(offset = consumerRecord.offset().toString(),
                    originalTopic = consumerRecord.topic(),
                    partition = consumerRecord.partition().toString(),
                    timestamp = consumerRecord.timestamp(),
                    exception = exception,
                    executingClass = executingClass,
                    key = toByteArray(consumerRecord.key()),
                    value = toByteArray(consumerRecord.value()),
                    databaseName = databaseName)
        }

        fun toByteArray(v:Any?) = try {
            when (v) {
                null -> null
                is ByteArray -> v
                is GenericRecord -> JSONUtils.writeValueAsBytes(mapOf("schema" to v.schema.toMap(), "record" to v.toMap()))
                else -> v.toString().toByteArray(Charsets.UTF_8)
            }
        } catch (e:Exception) {
            null
        }
    }
    fun toLogString() =
            """
ErrorData(originalTopic=$originalTopic, timestamp=$timestamp, partition=$partition, offset=$offset, exception=$exception, key=${key?.toString(Charsets.UTF_8)}, value=${value?.sliceArray(0..Math.min(value.size,200)-1)?.toString(Charsets.UTF_8)}, executingClass=$executingClass)
    """.trimIndent()

}

abstract class ErrorService(private val config: Map<String, Any> = emptyMap()) {

    data class ErrorConfig(val fail:Boolean=false, val log:Boolean=false, val logMessages:Boolean=false,
                           val dlqTopic:String? = null, val dlqHeaderPrefix:String = "", val dlqHeaders:Boolean = false, val dlqReplication: Int? = 3) {

        /*
        https://www.confluent.io/blog/kafka-connect-deep-dive-error-handling-dead-letter-queues
            "errors.retry.timeout": "-1",
            "errors.retry.delay.max.ms": "1000",

            "errors.tolerance": "all", "none" == fail-fast, abort sink task

            fail-fast for configuration errors (e.g. validate cypher statements on start)
            errors.tolerance = all -> silently ignore all bad messages

            org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator.execAndHandleError(RetryWithToleranceOperator.java


            "errors.log.enable": true,
            "errors.deadletterqueue.context.headers.enable"=true/false
            "errors.deadletterqueue.topic.name": "test-error-topic",
            "errors.deadletterqueue.topic.replication.factor": 1,
            "errors.log.include.messages": true,
        */

        companion object {
            const val TOLERANCE = "errors.tolerance"
            const val LOG = "errors.log.enable"
            const val LOG_MESSAGES = "errors.log.include.messages"
            const val DLQ_TOPIC = "errors.deadletterqueue.topic.name"
            const val DLQ_HEADERS = "errors.deadletterqueue.context.headers.enable"
            const val DLQ_HEADER_PREFIX = "errors.deadletterqueue.context.headers.prefix"
            const val DLQ_REPLICATION = "errors.deadletterqueue.topic.replication.factor"

            fun from(props: Properties) = from(props.toMap() as Map<String, Any>)

            fun boolean(v:Any?) = when (v) {
                null -> false
                "true" -> true
                "false" -> false
                is Boolean -> v
                else -> false
            }
            fun int(v:Any?) = when (v) {
                null -> 0
                is Int -> v
                is String -> v.toInt()
                else -> 0
            }

            fun from(config: Map<String, Any?>) =
                    ErrorConfig(
                            fail = config.getOrDefault(TOLERANCE, "none") == "none",
                            log = boolean(config.get(LOG)),
                            logMessages = boolean(config.get(LOG_MESSAGES)),
                            dlqTopic = config.get(DLQ_TOPIC) as String?,
                            dlqHeaders = boolean(config.get(DLQ_HEADERS)),
                            dlqHeaderPrefix = config.getOrDefault(DLQ_HEADER_PREFIX,"") as String,
                            dlqReplication = int(config.getOrDefault(DLQ_REPLICATION, 3)))
        }
    }

    abstract fun report(errorDatas: List<ErrorData>)

    open fun close() {}
}

class ProcessingError(val errorDatas: List<ErrorData>) :
        RuntimeException("Error processing ${errorDatas.size} messages\n"+errorDatas.map { it.toLogString() }.joinToString("\n"))
