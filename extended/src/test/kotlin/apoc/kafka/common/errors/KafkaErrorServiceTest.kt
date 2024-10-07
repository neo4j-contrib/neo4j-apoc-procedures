package apoc.kafka.common.errors

import apoc.kafka.service.errors.ErrorData
import apoc.kafka.service.errors.ErrorService
import apoc.kafka.service.errors.KafkaErrorService
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.internals.FutureRecordMetadata
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.utils.SystemTime
import org.junit.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals

class KafkaErrorServiceTest {
    @Test
    fun `should send the data to the DLQ`() {
        val producer: MockProducer<ByteArray, ByteArray> = Mockito.mock(MockProducer::class.java) as MockProducer<ByteArray, ByteArray>
        val counter = AtomicInteger(0)
        Mockito.`when`(producer.send(ArgumentMatchers.any<ProducerRecord<ByteArray, ByteArray>>())).then {
            counter.incrementAndGet()
            FutureRecordMetadata(null, 0, RecordBatch.NO_TIMESTAMP, 0L, 0, 0, SystemTime())
        }
        val dlqService = KafkaErrorService(producer, ErrorService.ErrorConfig(fail=false,dlqTopic = "dlqTopic"), { s, e -> })
        dlqService.report(listOf(dlqData()))
        assertEquals(1, counter.get())
        dlqService.close()
    }


    @Test
    fun `should create the header map`() {
        val producer: MockProducer<ByteArray, ByteArray> = Mockito.mock(MockProducer::class.java) as MockProducer<ByteArray, ByteArray>
        val dlqService = KafkaErrorService(producer, ErrorService.ErrorConfig(fail=false, dlqTopic = "dlqTopic",dlqHeaders = true), { s, e -> })
        val dlqData = dlqData()
        val map = dlqService.populateContextHeaders(dlqData)
        assertEquals(String(map["topic"]!!), dlqData.originalTopic)
        assertEquals(String(map["partition"]!!), dlqData.partition)
        assertEquals(String(map["offset"]!!), dlqData.offset)
        assertEquals(String(map["class.name"]!!), KafkaErrorServiceTest::class.java.name)
        val exception = dlqData.exception!!
        assertEquals(String(map["exception.class.name"]!!), exception::class.java.name)
        assertEquals(String(map["exception.message"]!!), exception.message)
        assertEquals(String(map["exception.stacktrace"]!!), ExceptionUtils.getStackTrace(exception))

    }

    private fun dlqData(): ErrorData {
        val offset = "0"
        val originalTopic = "topicName"
        val partition = "1"
        val timestamp = System.currentTimeMillis()
        val exception = RuntimeException("Test")
        val key = "KEY"
        val value = "VALUE"
        val databaseName = "myDb"
        return ErrorData(
                offset = offset,
                originalTopic = originalTopic,
                partition = partition,
                timestamp = timestamp,
                exception = exception,
                executingClass = KafkaErrorServiceTest::class.java,
                key = key.toByteArray(),
                value = value.toByteArray(),
                databaseName = databaseName
        )
    }

    @Test
    fun `should log DQL data`() {
        val log = { s:String,e:Exception? -> run {
            RuntimeException("Test")
            Unit
        }}
        val logService = KafkaErrorService(Properties(),ErrorService.ErrorConfig(fail = false,log=true), log)
        logService.report(listOf(dlqData()))
    }
}