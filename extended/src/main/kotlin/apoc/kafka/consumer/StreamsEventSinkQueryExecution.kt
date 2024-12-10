package apoc.kafka.consumer

import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import apoc.kafka.extensions.execute
import apoc.kafka.service.StreamsSinkService
import apoc.kafka.service.StreamsStrategyStorage
import apoc.kafka.consumer.utils.ConsumerUtils

class NotInWriteableInstanceException(message: String): RuntimeException(message)

class StreamsEventSinkQueryExecution(private val db: GraphDatabaseAPI,
                                     private val log: Log,
                                     streamsStrategyStorage: StreamsStrategyStorage):
        StreamsSinkService(streamsStrategyStorage) {

    override fun write(query: String, params: Collection<Any>) {
        if (params.isEmpty()) return
        if (ConsumerUtils.isWriteableInstance(db)) {
            db.execute(query, mapOf("events" to params)) {
                if (log.isDebugEnabled) {
                    log.debug("Query statistics:\n${it.queryStatistics}")
                }
            }
        } else {
            if (log.isDebugEnabled) {
                log.debug("Not writeable instance")
            }
            NotInWriteableInstanceException("Not writeable instance")
        }
    }
}
