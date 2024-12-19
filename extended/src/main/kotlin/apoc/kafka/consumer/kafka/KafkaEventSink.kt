package apoc.kafka.consumer.kafka

import apoc.kafka.consumer.StreamsEventConsumer
import apoc.kafka.consumer.StreamsEventConsumerFactory
import apoc.kafka.events.KafkaStatus
import apoc.kafka.extensions.isDefaultDb
import kotlinx.coroutines.Job
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log

class KafkaEventSink(private val db: GraphDatabaseAPI) {

    private val mutex = Mutex()

    private var job: Job? = null


    fun getEventConsumerFactory(): StreamsEventConsumerFactory {
        return object: StreamsEventConsumerFactory() {
            override fun createStreamsEventConsumer(config: Map<String, String>, log: Log, topics: Set<Any>): StreamsEventConsumer {
                val dbName = db.databaseName()
                val kafkaConfig = KafkaSinkConfiguration.from(config, dbName, db.isDefaultDb())
                val topics1 = topics as Set<String>
                return if (kafkaConfig.enableAutoCommit) {
                    KafkaAutoCommitEventConsumer(kafkaConfig, log, topics1, dbName)
                } else {
                    KafkaManualCommitEventConsumer(kafkaConfig, log, topics1, dbName)
                }
            }
        }
    }
    
    fun status(): KafkaStatus = runBlocking {
        mutex.withLock(job) {
            status(job)
        }
    }

    private fun status(job: Job?): KafkaStatus = when (job?.isActive) {
        true -> KafkaStatus.RUNNING
        else -> KafkaStatus.STOPPED
    }

}
