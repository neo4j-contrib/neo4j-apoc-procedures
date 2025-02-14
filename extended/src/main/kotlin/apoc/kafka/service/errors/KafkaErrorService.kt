package apoc.kafka.service.errors

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.neo4j.util.VisibleForTesting
import apoc.kafka.utils.KafkaUtil.validateConnection
import java.util.*

class KafkaErrorService(private val producer: Producer<ByteArray, ByteArray>?, private val errorConfig: ErrorConfig, private val log: (String, Exception?)->Unit): ErrorService() {

    constructor(config: Properties, errorConfig: ErrorConfig,
                log: (String, Exception?) -> Unit) : this(producer(errorConfig, config, log), errorConfig, log)

    companion object {
        private fun producer(errorConfig: ErrorConfig, config: Properties, log: (String, Exception?) -> Unit) =
                errorConfig.dlqTopic?.let {
                    try {
                        val bootstrapServers = config.getOrDefault(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "").toString()
                        validateConnection(bootstrapServers, CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, false)
                        config[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java.name
                        config[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java.name
                        KafkaProducer<ByteArray, ByteArray>(config)
                    } catch (e: Exception) {
                        log("Cannot initialize the custom DLQ because of the following exception: ", e)
                        null
                    }
                }
    }

    override fun report(errorDatas: List<ErrorData>) {
        if (errorConfig.fail) throw ProcessingError(errorDatas)
        if (errorConfig.log) {
            if (errorConfig.logMessages) {
                errorDatas.forEach{log(it.toLogString(),it.exception)}
            } else {
                errorDatas.map { it.exception }.distinct().forEach{log("Error processing ${errorDatas.size} messages",it)}
            }
        }

        errorDatas.forEach { dlqData ->
            producer?.let {
                try {
                    val producerRecord = if (dlqData.timestamp == RecordBatch.NO_TIMESTAMP) {
                        ProducerRecord(errorConfig.dlqTopic, null, dlqData.key, dlqData.value)
                    } else {
                        ProducerRecord(errorConfig.dlqTopic, null, dlqData.timestamp, dlqData.key, dlqData.value)
                    }
                    if (errorConfig.dlqHeaders) {
                        val producerHeader = producerRecord.headers()
                        populateContextHeaders(dlqData).forEach { (key, value) -> producerHeader.add(key, value) }
                    }
                    it.send(producerRecord)
                } catch (e: Exception) {
                    log("Error writing to DLQ $e: ${dlqData.toLogString()}", e) // todo only the first or all
                }
            }
        }
    }

    @VisibleForTesting
    fun populateContextHeaders(errorData: ErrorData): Map<String, ByteArray> {
        fun prefix(suffix: String) = errorConfig.dlqHeaderPrefix + suffix

        val headers = mutableMapOf(
        prefix("topic") to errorData.originalTopic.toByteArray(),
        prefix("partition") to errorData.partition.toByteArray(),
        prefix("offset") to errorData.offset.toByteArray())

        if (!errorData.databaseName.isNullOrBlank()) {
            headers[prefix("databaseName")] = errorData.databaseName.toByteArray()
        }

        if (errorData.executingClass != null) {
            headers[prefix("class.name")] = errorData.executingClass.name.toByteArray()
        }
        if (errorData.exception != null) {
            headers[prefix("exception.class.name")] = errorData.exception.javaClass.name.toByteArray()
            if (errorData.exception.message != null) {
                headers[prefix("exception.message")] = errorData.exception.message.toString().toByteArray()
            }
            headers[prefix("exception.stacktrace")] = ExceptionUtils.getStackTrace(errorData.exception).toByteArray()
        }
        return headers
    }


    override fun close() {
        this.producer?.close()
    }

}