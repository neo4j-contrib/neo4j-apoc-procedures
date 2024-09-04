package apoc.kafka.consumer

import apoc.kafka.config.StreamsConfig
import apoc.kafka.consumer.kafka.KafkaEventSink
import apoc.kafka.consumer.kafka.KafkaSinkConfiguration
import apoc.kafka.consumer.procedures.StreamsSinkProcedures
import apoc.kafka.consumer.utils.ConsumerUtils
import apoc.kafka.extensions.isDefaultDb
import apoc.kafka.utils.KafkaUtil
import apoc.kafka.utils.KafkaUtil.getProducerProperties
import kotlinx.coroutines.sync.Mutex
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log

class StreamsSinkConfigurationListener(private val db: GraphDatabaseAPI,
                                       private val log: Log)  {

//    private val mutex = Mutex()
//
    var eventSink: KafkaEventSink? = null
//
//    private val streamsTopicService = StreamsTopicService()
//
//    private var lastConfig: KafkaSinkConfiguration? = null
//
//    private val producerConfig = getProducerProperties()
//
//    private fun KafkaSinkConfiguration.excludeSourceProps() = this.asProperties()
//        ?.filterNot { producerConfig.contains(it.key) || it.key.toString().startsWith("apoc.kafka.source") }


    fun shutdown() {
//        val isShuttingDown = eventSink != null
//        if (isShuttingDown) {
//            log.info("[Sink] Shutting down the Streams Sink Module")
//        }
//        eventSink?.stop()
//        eventSink = null
        StreamsSinkProcedures.unregisterStreamsEventSink(db)
//        if (isShuttingDown) {
//            log.info("[Sink] Shutdown of the Streams Sink Module completed")
//        }
    }

    fun start(configMap: Map<String, String>) {

        eventSink = StreamsEventSinkFactory
            .getStreamsEventSink(configMap,
               // streamsQueryExecution,
               // streamsTopicService,
                log,
                db)

//        log.info("[Sink] Registering the Streams Sink procedures")
        StreamsSinkProcedures.registerStreamsEventSink(db, eventSink!!)
    }
    
}