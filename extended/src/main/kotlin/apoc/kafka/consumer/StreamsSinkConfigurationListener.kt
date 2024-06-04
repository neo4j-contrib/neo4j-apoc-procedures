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
//        lastConfig = KafkaSinkConfiguration.create(StreamsConfig.getConfiguration(), db.databaseName(), db.isDefaultDb())
//        val streamsSinkConfiguration = lastConfig!!.sinkConfiguration
//        streamsTopicService.clearAll()
//        streamsTopicService.setAll(streamsSinkConfiguration.topics)
//
//        val neo4jStrategyStorage = Neo4jStreamsStrategyStorage(streamsTopicService, configMap, db)
//        val streamsQueryExecution = StreamsEventSinkQueryExecution(db,
//            log, neo4jStrategyStorage)
//
        eventSink = StreamsEventSinkFactory
            .getStreamsEventSink(configMap,
               // streamsQueryExecution,
               // streamsTopicService,
                log,
                db)
//        try {
//            if (streamsSinkConfiguration.enabled) {
//                log.info("[Sink] The Streams Sink module is starting")
//                if (KafkaUtil.isCluster(db)) {
//                    initSinkModule(streamsSinkConfiguration)
//                } else {
//                    runInASingleInstance(streamsSinkConfiguration)
//                }
//            }
//        } catch (e: Exception) {
//            log.warn("Cannot start the Streams Sink module because the following exception", e)
//        }
//
//        log.info("[Sink] Registering the Streams Sink procedures")
        StreamsSinkProcedures.registerStreamsEventSink(db, eventSink!!)
    }

//    private fun initSink() {
//        eventSink?.start()
//        eventSink?.printInvalidTopics()
//    }
//
//    private fun runInASingleInstance(streamsSinkConfiguration: StreamsSinkConfiguration) {
//        // check if is writeable instance
//        ConsumerUtils.executeInWriteableInstance(db) {
//            if (streamsSinkConfiguration.clusterOnly) {
//                log.info("""
//                        |Cannot init the Streams Sink module as is forced to work only in a cluster env, 
//                        |please check the value of `${StreamsConfig.CLUSTER_ONLY}`
//                    """.trimMargin())
//            } else {
//                initSinkModule(streamsSinkConfiguration)
//            }
//        }
//    }
//
//    private fun initSinkModule(streamsSinkConfiguration: StreamsSinkConfiguration) {
//            initSink()
//    }
}