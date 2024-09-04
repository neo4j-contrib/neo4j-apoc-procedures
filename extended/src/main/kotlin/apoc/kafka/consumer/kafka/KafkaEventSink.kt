package apoc.kafka.consumer.kafka

import apoc.kafka.config.StreamsConfig
import apoc.kafka.consumer.StreamsEventConsumer
import apoc.kafka.consumer.StreamsEventConsumerFactory
import apoc.kafka.consumer.StreamsEventSinkQueryExecution
//import apoc.kafka.consumer.StreamsSinkConfiguration
import apoc.kafka.consumer.StreamsTopicService
import apoc.kafka.consumer.utils.ConsumerUtils
import apoc.kafka.events.StreamsPluginStatus
import apoc.kafka.extensions.isDefaultDb
import apoc.kafka.utils.KafkaUtil
import apoc.kafka.utils.KafkaUtil.getInvalidTopicsError
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.kafka.common.errors.WakeupException
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log

class KafkaEventSink(//private val config: Map<String, String>,
    //private val queryExecution: StreamsEventSinkQueryExecution,
    //              private val streamsTopicService: StreamsTopicService,
    //               private val log: Log,
                     private val db: GraphDatabaseAPI) {

    private val mutex = Mutex()

    private lateinit var eventConsumer: KafkaEventConsumer
    private var job: Job? = null

//    val streamsSinkConfiguration: StreamsSinkConfiguration = StreamsSinkConfiguration.from(configMap = config,
//        dbName = db.databaseName(), isDefaultDb = db.isDefaultDb())
//
//    private val streamsConfig: StreamsSinkConfiguration = StreamsSinkConfiguration.from(configMap = config,
//        dbName = db.databaseName(), isDefaultDb = db.isDefaultDb())

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
    
    fun status(): StreamsPluginStatus = runBlocking {
        mutex.withLock(job) {
            status(job)
        }
    }

    private fun status(job: Job?): StreamsPluginStatus = when (job?.isActive) {
        true -> StreamsPluginStatus.RUNNING
        else -> StreamsPluginStatus.STOPPED
    }

}
