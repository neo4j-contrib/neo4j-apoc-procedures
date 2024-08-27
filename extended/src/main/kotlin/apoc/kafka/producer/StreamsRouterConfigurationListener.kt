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
                                         private val log: Log) /*: ConfigurationLifecycleListener*/ {
    private val mutex = Mutex()

    private var txHandler: StreamsTransactionEventHandler? = null
    private var streamsConstraintsService: StreamsConstraintsService? = null
    private var streamsEventRouter: StreamsEventRouter? = null
    private var streamsEventRouterConfiguration: StreamsEventRouterConfiguration? = null

    private var lastConfig: KafkaConfiguration? = null

    private val consumerConfig = getConsumerProperties()

    private fun KafkaConfiguration.excludeSinkProps() = this.asProperties()
        ?.filterNot { consumerConfig.contains(it.key)
                || it.key.toString().startsWith("apoc.kafka.sink")
                // these are not yet used by the streams Source module
                || it.key == "apoc.kafka.cluster.only"
                || it.key == "apoc.kafka.check.apoc.timeout"
                || it.key == "apoc.kafka.check.apoc.interval" }

//    override fun onShutdown() {
//        runBlocking {
//            mutex.withLock {
//                shutdown()
//            }
//        }
//    }

    // visible for testing
    fun isConfigurationChanged(configMap: Map<String, String>) = when (configMap
        .getOrDefault("apoc.kafka.router", "apoc.kafka.kafka.KafkaEventRouter")) {
        "apoc.kafka.kafka.KafkaEventRouter" ->  {
            // we validate all properties except for the ones related to the Consumer
            // we use this strategy because there are some properties related to the Confluent Platform
            // that we're not able to track from the Apache Packages
            // i.e. the Schema Registry
            val config = KafkaConfiguration.create(configMap).excludeSinkProps()
            val lastConfig = lastConfig?.excludeSinkProps()
            val streamsConfig = StreamsEventRouterConfiguration.from(configMap, db.databaseName(), isDefaultDb = db.isDefaultDb(), log)
            config != lastConfig || streamsConfig != streamsEventRouterConfiguration
        }
        else -> true
    }

    fun shutdown() {
//        val isShuttingDown = txHandler?.status() == StreamsPluginStatus.RUNNING
//        if (isShuttingDown) {
//            log.info("[Sink] Shutting down the Streams Source Module")
//        }
        if (streamsEventRouterConfiguration?.enabled == true) {
            streamsConstraintsService?.close()
            streamsEventRouter?.stop()
            streamsEventRouter = null
            PublishProcedures.unregister(db)
            txHandler?.stop()
            txHandler = null
        }
//        if (isShuttingDown) {
//            log.info("[Source] Shutdown of the Streams Source Module completed")
//        }
    }

    fun start(configMap: Map<String, String>) {
        lastConfig = KafkaConfiguration.create(configMap)
        streamsEventRouterConfiguration = StreamsEventRouterConfiguration.from(configMap, db.databaseName(), isDefaultDb = db.isDefaultDb(), log)
        streamsEventRouter = StreamsEventRouterFactory.getStreamsEventRouter(configMap, db, log)
        streamsConstraintsService = StreamsConstraintsService(db, streamsEventRouterConfiguration!!.schemaPollingInterval)
        if (streamsEventRouterConfiguration?.enabled == true || streamsEventRouterConfiguration?.proceduresEnabled == true) {
            streamsConstraintsService!!.start()
            streamsEventRouter!!.start()
        }
        txHandler = StreamsTransactionEventHandler(streamsEventRouter!!, db, streamsConstraintsService!!)
        if (streamsEventRouterConfiguration?.enabled == true) {
            streamsEventRouter!!.printInvalidTopics()
            txHandler!!.start()
        }
        PublishProcedures.register(db, streamsEventRouter!!, txHandler!!)
        log.info("[Source] Streams Source module initialised")
    }

//    override fun onConfigurationChange(evt: EventType, config: ImmutableConfiguration) {
//        if (config.isEmpty) {
//            if (log.isDebugEnabled) {
//                log.debug("[Source] Configuration is empty")
//            }
//            return
//        }
//        runBlocking {
//            mutex.withLock {
//                log.info("[Source] An event change is detected ${evt.name}")
//                val configMap = ConfigurationLifecycleUtils.toMap(config)
//                    .mapValues { it.value.toString() }
//                if (!isConfigurationChanged(configMap)) {
//                    log.info("[Source] The configuration is not changed so the module will not restarted")
//                    return@runBlocking
//                }
//                log.info("[Source] Shutting down the Streams Source Module")
//                shutdown()
//                log.info("[Source] Initialising the Streams Source module")
//                if (log.isDebugEnabled) {
//                    log.debug("[Source] The new configuration is: $configMap")
//                }
//                start(configMap)
//            }
//        }
//    }
}