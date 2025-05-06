package apoc.kafka.consumer

import apoc.kafka.consumer.kafka.KafkaEventSink
import apoc.kafka.consumer.procedures.StreamsSinkProcedures
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log

class StreamsSinkConfigurationListener(private val db: GraphDatabaseAPI,
                                       private val log: Log)  {

    var eventSink: KafkaEventSink? = null


    fun shutdown() {
        StreamsSinkProcedures.unregisterStreamsEventSink(db)

    }

    fun start(configMap: Map<String, String>) {

        eventSink = StreamsEventSinkFactory
            .getStreamsEventSink(configMap,
                log,
                db)

        StreamsSinkProcedures.registerStreamsEventSink(db, eventSink!!)
    }
    
}