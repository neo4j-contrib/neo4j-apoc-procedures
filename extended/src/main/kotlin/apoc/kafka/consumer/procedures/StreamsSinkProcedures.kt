package apoc.kafka.consumer.procedures

import apoc.kafka.config.StreamsConfig
import apoc.kafka.consumer.StreamsEventConsumer
//import apoc.kafka.consumer.StreamsSinkConfiguration
import apoc.kafka.consumer.kafka.KafkaEventSink
import apoc.kafka.events.StreamsPluginStatus
import apoc.kafka.extensions.isDefaultDb
import apoc.kafka.utils.KafkaUtil
import apoc.kafka.utils.KafkaUtil.checkEnabled
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.commons.lang3.exception.ExceptionUtils
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
import java.util.stream.Collectors
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
//            val configuration = getStreamsEventSink(db!!)!!
//                .getEventSinkConfigMapper()
//                .convert(config = properties)

            val configuration = StreamsConfig.getConfiguration(properties)
            readData(topic, config ?: emptyMap(), configuration)
        }
    }

//    @Procedure("apoc.kafka.sink.start")
//    fun sinkStart(): Stream<KeyValueResult> {
//        checkEnabled()
//        return checkLeader {
//            try {
//                getStreamsEventSink(db!!)?.start()
//                sinkStatus()
//            } catch (e: Exception) {
//                log?.error("Cannot start the Sink because of the following exception", e)
//                Stream.concat(sinkStatus(),
//                        Stream.of(KeyValueResult("exception", ExceptionUtils.getStackTrace(e))))
//            }
//        }
//    }
//
//    @Procedure("apoc.kafka.sink.stop")
//    fun sinkStop(): Stream<KeyValueResult> {
//        checkEnabled()
//        return checkLeader {
//            try {
//                getStreamsEventSink(db!!)?.stop()
//                sinkStatus()
//            } catch (e: Exception) {
//                log?.error("Cannot stopped the Sink because of the following exception", e)
//                Stream.concat(sinkStatus(),
//                        Stream.of(KeyValueResult("exception", ExceptionUtils.getStackTrace(e))))
//            }
//        }
//    }
//
//    @Procedure("apoc.kafka.sink.restart")
//    fun sinkRestart(): Stream<KeyValueResult> {
//        val stopped = sinkStop().collect(Collectors.toList())
//        val hasError = stopped.any { it.name == "exception" }
//        if (hasError) {
//            return stopped.stream()
//        }
//        return sinkStart()
//    }
//
//    @Procedure("apoc.kafka.sink.config")
//    @Deprecated("Please use apoc.kafka.configuration.get")
//    fun sinkConfig(): Stream<KeyValueResult> {
//        checkEnabled()
//        return checkLeader {
//            StreamsSinkConfiguration
//                // todo - check that
////                    .from(configMap = StreamsConfig.getInstance(db!! as GraphDatabaseAPI)
//                    .from(configMap = StreamsConfig
//                        .getConfiguration().mapValues { it.value.toString() },
//                        dbName = db!!.databaseName(),
//                        isDefaultDb = db!!.isDefaultDb())
//                    .asMap()
//                    .entries.stream()
//                    .map { KeyValueResult(it.key, it.value) }
//        }
//    }
//
//    @Procedure("apoc.kafka.sink.status")
//    fun sinkStatus(): Stream<KeyValueResult> {
//        checkEnabled()
//        return run {
//            val value = (getStreamsEventSink(db!!)?.status() ?: StreamsPluginStatus.UNKNOWN).toString()
//            Stream.of(KeyValueResult("status", value))
//        }
//    }

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
                    println("coroutineContext = ${coroutineContext}")
                    consumer.read(cfg) { _, topicData ->
                        println("topicData = ${topicData}")
                        data.addAll(topicData.mapNotNull { it.value }.map { StreamResult(mapOf("data" to it)) })
                    }
                }
                println("coroutineContext = ${coroutineContext}")
                data.add(tombstone)
            } catch (e: Exception) {
                println("coroutineContext = "  
                + e.message)
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

        return StreamSupport.stream(QueueBasedSpliterator(data, tombstone, terminationGuard!!, timeout), false)
    }

    private fun createConsumer(consumerConfig: Map<String, String>, topic: String): StreamsEventConsumer = runBlocking {
        // todo - check that
        val copy = StreamsConfig.getConfiguration()
//        val copy = StreamsConfig.getInstance(db!! as GraphDatabaseAPI).getConfiguration()
            .filter { it.value is String }
            .mapValues { it.value.toString() }
            .toMutableMap()
        copy.putAll(consumerConfig)
        getStreamsEventSink(db!!)!!.getEventConsumerFactory()
                .createStreamsEventConsumer(copy, log!!, setOf(topic))
    }

    companion object {
        // todo - move in another class, similar to CypherProceduresHandler extends LifecycleAdapter implements AvailabilityListener {
//        fun initListeners(db: GraphDatabaseAPI?, log: Log?) {
//            // todo - move in another class, similar to CypherProcedureHandler
//            // todo - check if there is a better way, maybe put if(apoc.kafka.enabled=true) 
//            StreamsRouterConfigurationListener(db!!, log!!
//            ).start(StreamsConfig.getConfiguration())
//
//            StreamsSinkConfigurationListener(db!!, log!!
//            ).start(StreamsConfig.getConfiguration())
//        }
//        
        private val streamsEventSinkStore = ConcurrentHashMap<String, KafkaEventSink>()

        private fun getStreamsEventSink(db: GraphDatabaseService) = streamsEventSinkStore[KafkaUtil.getName(db)]

        fun registerStreamsEventSink(db: GraphDatabaseAPI, streamsEventSink: KafkaEventSink) {
            streamsEventSinkStore[KafkaUtil.getName(db)] = streamsEventSink
        }

        fun unregisterStreamsEventSink(db: GraphDatabaseAPI) = streamsEventSinkStore.remove(KafkaUtil.getName(db))

        fun hasStatus(db: GraphDatabaseAPI, status: StreamsPluginStatus) = getStreamsEventSink(db)?.status() == status

        fun isRegistered(db: GraphDatabaseAPI) = getStreamsEventSink(db) != null
    }
}