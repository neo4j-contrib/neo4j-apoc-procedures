package apoc.kafka.consumer.procedures

import apoc.kafka.config.StreamsConfig
import apoc.kafka.consumer.StreamsEventConsumer
import apoc.kafka.consumer.kafka.KafkaEventSink
import apoc.kafka.utils.KafkaUtil
import apoc.kafka.utils.KafkaUtil.checkEnabled
import apoc.util.QueueBasedSpliterator
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import org.neo4j.procedure.Context
import org.neo4j.procedure.Description
import org.neo4j.procedure.Mode
import org.neo4j.procedure.Name
import org.neo4j.procedure.Procedure
import org.neo4j.procedure.TerminationGuard
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.stream.Stream
import java.util.stream.StreamSupport

class StreamResult(@JvmField val event: Map<String, *>)
class KeyValueResult(@JvmField val name: String, @JvmField val value: Any?)

class StreamsSinkProcedures {
    
    
    @JvmField @Context
    var log: Log? = null

    @JvmField @Context
    var db: GraphDatabaseAPI? = null

    @JvmField @Context
    var terminationGuard: TerminationGuard? = null

    @Procedure(mode = Mode.READ, name = "apoc.kafka.consume")
    @Description("apoc.kafka.consume(topic, {timeout: <long value>, from: <string>, groupId: <string>, commit: <boolean>, partitions:[{partition: <number>, offset: <number>}]}) " +
            "YIELD event - Allows to consume custom topics")
    fun consume(@Name("topic") topic: String?,
                @Name(value = "config", defaultValue = "{}") config: Map<String, Any>?): Stream<StreamResult> = runBlocking {
        checkEnabled()
        if (topic.isNullOrEmpty()) {
            log?.info("Topic empty, no message sent")
            Stream.empty<StreamResult>()
        } else {
            val properties = config?.mapValues { it.value.toString() } ?: emptyMap()

            val configuration = StreamsConfig.getConfiguration(properties)
            readData(topic, config ?: emptyMap(), configuration)
        }
    }

    private fun checkLeader(lambda: () -> Stream<KeyValueResult>): Stream<KeyValueResult> = if (KafkaUtil.isWriteableInstance(db as GraphDatabaseAPI)) {
        lambda()
    } else {
        Stream.of(KeyValueResult("error", "You can use this procedure only in the LEADER or in a single instance configuration."))
    }

    private fun readData(topic: String, procedureConfig: Map<String, Any>, consumerConfig: Map<String, String>): Stream<StreamResult?> {
        val cfg = procedureConfig.mapValues { if (it.key != "partitions") it.value else mapOf(topic to it.value) }
        val timeout = cfg.getOrDefault("timeout", 1000).toString().toLong()
        val data = ArrayBlockingQueue<StreamResult>(1000)
        val tombstone = StreamResult(emptyMap<String, Any>())
        GlobalScope.launch(Dispatchers.IO) {
            val consumer = createConsumer(consumerConfig, topic)
            consumer.start()
            try {
                val start = System.currentTimeMillis()
                while ((System.currentTimeMillis() - start) < timeout) {
                    consumer.read(cfg) { _, topicData ->
                        data.addAll(topicData.mapNotNull { it.value }.map { StreamResult(mapOf("data" to it)) })
                    }
                }
                data.add(tombstone)
            } catch (e: Exception) {
                if (log?.isDebugEnabled!!) {
                    log?.error("Error while consuming data", e)
                }
            } finally {
                consumer.stop()
            }
        }
        if (log?.isDebugEnabled!!) {
            log?.debug("Data retrieved from topic $topic after $timeout milliseconds: $data")
        }

        return StreamSupport.stream(QueueBasedSpliterator(data, tombstone, terminationGuard, timeout.toInt()), false)
    }

    private fun createConsumer(consumerConfig: Map<String, String>, topic: String): StreamsEventConsumer = runBlocking {
        val copy = StreamsConfig.getConfiguration()
            .filter { it.value is String }
            .mapValues { it.value }
            .toMutableMap()
        copy.putAll(consumerConfig)
        getStreamsEventSink(db!!)!!.getEventConsumerFactory()
                .createStreamsEventConsumer(copy, log!!, setOf(topic))
    }

    companion object {
        private val streamsEventSinkStore = ConcurrentHashMap<String, KafkaEventSink>()

        private fun getStreamsEventSink(db: GraphDatabaseService) = streamsEventSinkStore[KafkaUtil.getName(db)]

        fun registerStreamsEventSink(db: GraphDatabaseAPI, streamsEventSink: KafkaEventSink) {
            streamsEventSinkStore[KafkaUtil.getName(db)] = streamsEventSink
        }

        fun unregisterStreamsEventSink(db: GraphDatabaseAPI) = streamsEventSinkStore.remove(KafkaUtil.getName(db))

    }
}