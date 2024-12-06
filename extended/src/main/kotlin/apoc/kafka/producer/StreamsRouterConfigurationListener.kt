package apoc.kafka.producer

import apoc.kafka.PublishProcedures
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import apoc.kafka.producer.kafka.KafkaConfiguration
import apoc.kafka.producer.kafka.KafkaEventRouter

class StreamsRouterConfigurationListener(private val db: GraphDatabaseAPI,
                                         private val log: Log) {

    private var streamsEventRouter: KafkaEventRouter? = null
    private var streamsEventRouterConfiguration: StreamsEventRouterConfiguration? = null

    private var lastConfig: KafkaConfiguration? = null
    
    fun shutdown() {
        if (streamsEventRouterConfiguration?.enabled == true) {
            streamsEventRouter?.stop()
            streamsEventRouter = null
            PublishProcedures.unregister(db)
        }
    }

    fun start(configMap: Map<String, String>) {
        lastConfig = KafkaConfiguration.create(configMap)
        streamsEventRouterConfiguration = StreamsEventRouterConfiguration.from(configMap, db.databaseName(), log)
        streamsEventRouter = KafkaEventRouter(configMap, db, log)
        if (streamsEventRouterConfiguration?.enabled == true || streamsEventRouterConfiguration?.proceduresEnabled == true) {
            streamsEventRouter!!.start()
        }
        PublishProcedures.register(db, streamsEventRouter!!)
        log.info("[Source] Streams Source module initialised")
    }
}