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

//    fun start() = runBlocking { // TODO move to the abstract class
//        if (streamsConfig.clusterOnly && !KafkaUtil.isCluster(db)) {
//            if (log.isDebugEnabled) {
//                log.info("""
//                        |Cannot init the Kafka Sink module as is forced to work only in a cluster env, 
//                        |please check the value of `${StreamsConfig.CLUSTER_ONLY}`
//                    """.trimMargin())
//            }
//            return@runBlocking
//        }
//        val topics = streamsTopicService.getTopics()
//        val isWriteableInstance = ConsumerUtils.isWriteableInstance(db)
//        if (!streamsConfig.enabled) {
//            if (topics.isNotEmpty() && isWriteableInstance) {
//                log.warn("You configured the following topics: $topics, in order to make the Sink work please set ${StreamsConfig.SINK_ENABLED}=true")
//            }
//            log.info("The Kafka Sink is disabled")
//            return@runBlocking
//        }
//        if (topics.isEmpty()) {
//            if (isWriteableInstance) {
//                log.warn("The Kafka Sink will not start because no topics are provided")
//            }
//            return@runBlocking
//        }
//        log.info("Starting the Kafka Sink")
//        mutex.withLock(job) {
//            if (StreamsPluginStatus.RUNNING == status(job)) {
//                if (log.isDebugEnabled) {
//                    log.debug("Kafka Sink is already started.")
//                }
//                return@runBlocking
//            }
//            try {
//                job = createJob(streamsConfig, topics)
//            } catch (e: Exception) {
//                log.error("The Kafka Sink will not start, cannot create the sink job because of the following exception:", e)
//                return@runBlocking
//            }
//        }
//        if (isWriteableInstance) {
//            if (log.isDebugEnabled) {
//                streamsTopicService.getAll().forEach {
//                    log.debug("Subscribed topics for type ${it.key} are: ${it.value}")
//                }
//            } else {
//                log.info("Subscribed topics: $topics")
//            }
//        } else {
//            if (log.isDebugEnabled) {
//                log.info("Not a writeable instance")
//            }
//        }
//        log.info("Kafka Sink started")
//    }
//
//    fun stop() = runBlocking { // TODO move to the abstract class
//        log.info("Stopping Kafka Sink daemon Job")
//        mutex.withLock(job) {
//            if (status(job) == StreamsPluginStatus.STOPPED) {
//                return@runBlocking
//            }
//            KafkaUtil.ignoreExceptions({
//                runBlocking {
//                    if (job?.isActive == true) {
//                        eventConsumer.wakeup()
//                        job?.cancelAndJoin()
//                    }
//                    log.info("Kafka Sink daemon Job stopped")
//                }
//            }, UninitializedPropertyAccessException::class.java)
//        }
//        Unit
//    }
//
//    private fun createJob(streamsConfig: StreamsSinkConfiguration, topics: Set<String>): Job {
//        log.info("Creating Sink daemon Job")
//        return GlobalScope.launch(Dispatchers.IO) { // TODO improve exception management
//            try {
//                eventConsumer = getEventConsumerFactory()
//                    .createStreamsEventConsumer(config, log, topics) as KafkaEventConsumer
//                eventConsumer.start()
//                while (isActive) {
//                    val timeMillis = if (ConsumerUtils.isWriteableInstance(db)) {
//                        eventConsumer.read { topic, data ->
//                            if (log.isDebugEnabled) {
//                                log.debug("Reading data from topic $topic")
//                            }
//                            queryExecution.writeForTopic(topic, data)
//                        }
//                        streamsConfig.pollInterval
//                    } else {
//                        val timeMillis = streamsConfig.checkWriteableInstanceInterval
//                        if (log.isDebugEnabled) {
//                            log.debug("Not in a writeable instance, new check in $timeMillis millis")
//                        }
//                        timeMillis
//                    }
//                    delay(timeMillis)
//                }
//            } catch (e: Exception) {
//                when (e) {
//                    is CancellationException, is WakeupException -> null
//                    else -> {
//                        val message = e.message ?: "Generic error, please check the stack trace: "
//                        log.error(message, e)
//                    }
//                }
//            } finally {
//                KafkaUtil.ignoreExceptions({ eventConsumer.stop() }, Exception::class.java)
//            }
//        }
//    }
//
//    fun printInvalidTopics() {
//        KafkaUtil.ignoreExceptions({
//            if (eventConsumer.invalidTopics().isNotEmpty()) {
//                log.warn(getInvalidTopicsError(eventConsumer.invalidTopics()))
//            }
//        }, UninitializedPropertyAccessException::class.java)
//    }

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
