package apoc.kafka.producer.kafka

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.admin.AdminClient
import org.neo4j.logging.Log
import apoc.kafka.utils.KafkaUtil.isAutoCreateTopicsEnabled
import apoc.kafka.utils.KafkaUtil.getInvalidTopics
import apoc.kafka.utils.KafkaUtil
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

class KafkaAdminService(private val props: KafkaConfiguration, /*private val allTopics: List<String>, */private val log: Log) {
    private val client = AdminClient.create(props.asProperties())
    private val kafkaTopics: MutableSet<String> = Collections.newSetFromMap(ConcurrentHashMap<String, Boolean>())
    private val isAutoCreateTopicsEnabled = isAutoCreateTopicsEnabled(client)
    private lateinit var job: Job

    fun start() {
        if (!isAutoCreateTopicsEnabled) {
            job = GlobalScope.launch(Dispatchers.IO) {
                while (isActive) {
                    try {
                        kafkaTopics += client.listTopics().names().get()
                    } catch (e: Exception) {
                        log.warn("""Cannot retrieve valid topics because the following exception, 
                            |next attempt is in ${props.topicDiscoveryPollingInterval} ms:
                        """.trimMargin(), e)
                    }
                    delay(props.topicDiscoveryPollingInterval)
                }
                client.close()
            }
        }
    }

    fun stop() {
        KafkaUtil.ignoreExceptions({
            runBlocking {
                job.cancelAndJoin()
            }
        }, UninitializedPropertyAccessException::class.java)
    }

    fun isValidTopic(topic: String) = when (isAutoCreateTopicsEnabled) {
        true -> true
        else -> kafkaTopics.contains(topic)
    }

//    fun getInvalidTopics() = getInvalidTopics(client, allTopics)
}