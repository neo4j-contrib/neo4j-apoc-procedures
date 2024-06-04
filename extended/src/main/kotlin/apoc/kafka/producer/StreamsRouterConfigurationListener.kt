package apoc.kafka.producer

import apoc.kafka.PublishProcedures
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.commons.configuration2.ImmutableConfiguration
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import org.neo4j.plugin.configuration.ConfigurationLifecycleUtils
import org.neo4j.plugin.configuration.EventType
import org.neo4j.plugin.configuration.listners.ConfigurationLifecycleListener
import apoc.kafka.events.StreamsPluginStatus
import apoc.kafka.extensions.isDefaultDb
import apoc.kafka.producer.kafka.KafkaConfiguration
//import apoc.kafka.producer.procedures.StreamsProcedures
import apoc.kafka.utils.KafkaUtil
import apoc.kafka.utils.KafkaUtil.getConsumerProperties

class StreamsRouterConfigurationListener(private val db: GraphDatabaseAPI,
                                         private val log: Log)  {
    private val mutex = Mutex()

//    private var txHandler: StreamsTransactionEventHandler? = null
//    private var streamsConstraintsService: StreamsConstraintsService? = null
    private var streamsEventRouter: StreamsEventRouter? = null
//    private var streamsEventRouterConfiguration: StreamsEventRouterConfiguration? = null

    private var lastConfig: KafkaConfiguration? = null
    
    
    fun shutdown() {
//        val isShuttingDown = txHandler?.status() == StreamsPluginStatus.RUNNING
//        if (isShuttingDown) {
//            log.info("[Sink] Shutting down the Streams Source Module")
//        }
//        if (streamsEventRouterConfiguration?.enabled == true) {
//            streamsConstraintsService?.close()
            streamsEventRouter?.stop()
            streamsEventRouter = null
            PublishProcedures.unregister(db)
//            txHandler?.stop()
//            txHandler = null
//        }
//        if (isShuttingDown) {
//            log.info("[Source] Shutdown of the Streams Source Module completed")
//        }
    }

    fun start(configMap: Map<String, String>) {
        lastConfig = KafkaConfiguration.create(configMap)
//        streamsEventRouterConfiguration = StreamsEventRouterConfiguration.from(configMap, db.databaseName(), isDefaultDb = db.isDefaultDb(), log)
        streamsEventRouter = StreamsEventRouterFactory.getStreamsEventRouter(configMap, db, log)
//        streamsConstraintsService = StreamsConstraintsService(db, streamsEventRouterConfiguration!!.schemaPollingInterval)
//        if (streamsEventRouterConfiguration?.enabled == true || streamsEventRouterConfiguration?.proceduresEnabled == true) {
//            streamsConstraintsService!!.start()
            streamsEventRouter!!.start()
//        }
//        txHandler = StreamsTransactionEventHandler(streamsEventRouter!!, db, streamsConstraintsService!!)
//        if (streamsEventRouterConfiguration?.enabled == true) {
            streamsEventRouter!!.printInvalidTopics()
//            txHandler!!.start()
//        }
        PublishProcedures.register(db, streamsEventRouter!!/*, txHandler!!*/)
        log.info("[Source] Streams Source module initialised")
    }
}