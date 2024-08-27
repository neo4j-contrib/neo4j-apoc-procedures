package apoc.kafka

import apoc.kafka.producer.StreamsEventRouter
import apoc.kafka.producer.StreamsTransactionEventHandler
//import apoc.kafka.producer.StreamsTransactionEventHandler
import apoc.kafka.producer.events.StreamsEventBuilder
import apoc.kafka.utils.KafkaUtil
import apoc.kafka.utils.KafkaUtil.checkEnabled
import kotlinx.coroutines.runBlocking
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import org.neo4j.procedure.Context
import org.neo4j.procedure.Description
import org.neo4j.procedure.Mode
import org.neo4j.procedure.Name
import org.neo4j.procedure.Procedure
import java.util.concurrent.ConcurrentHashMap
import java.util.stream.Stream

data class StreamPublishResult(@JvmField val value: Map<String, Any>)

data class StreamsEventSinkStoreEntry(val eventRouter: StreamsEventRouter,
                                      val txHandler: StreamsTransactionEventHandler
)
class PublishProcedures {

    @JvmField @Context
    var db: GraphDatabaseAPI? = null

    @JvmField @Context var log: Log? = null

    @Procedure(mode = Mode.READ, name = "apoc.kafka.publish.sync")
    @Description("apoc.kafka.publish.sync(topic, payload, config) - Allows custom synchronous streaming from Neo4j to the configured stream environment")
    fun sync(@Name("topic") topic: String?, @Name("payload") payload: Any?,
             @Name(value = "config", defaultValue = "{}") config: Map<String, Any>?): Stream<StreamPublishResult> {
        checkEnabled()
        if (isTopicNullOrEmpty(topic)) {
            return Stream.empty()
        }
        checkPayloadNotNull(payload)

        val streamsEvent = buildStreamEvent(topic!!, payload!!)
        return getStreamsEventSinkStoreEntry().eventRouter
            .sendEventsSync(topic, listOf(streamsEvent), config ?: emptyMap())
            .map { StreamPublishResult(it) }
            .stream()
    }

    @Procedure(mode = Mode.READ, name = "apoc.kafka.publish")
    @Description("apoc.kafka.publish(topic, payload, config) - Allows custom streaming from Neo4j to the configured stream environment")
    fun publish(@Name("topic") topic: String?, @Name("payload") payload: Any?,
                @Name(value = "config", defaultValue = "{}") config: Map<String, Any>?) = runBlocking {
        checkEnabled()
        if (isTopicNullOrEmpty(topic)) {
            return@runBlocking
        }
        checkPayloadNotNull(payload)

        val streamsEvent = buildStreamEvent(topic!!, payload!!)
        getStreamsEventSinkStoreEntry().eventRouter.sendEvents(topic, listOf(streamsEvent), config ?: emptyMap())
    }

    private fun isTopicNullOrEmpty(topic: String?): Boolean {
        return if (topic.isNullOrEmpty()) {
            log?.info("Topic empty, no message sent")
            true
        } else {
            false
        }
    }

    private fun checkPayloadNotNull(payload: Any?) {
        if (payload == null) {
            log?.error("Payload empty, no message sent")
            throw RuntimeException("Payload may not be null")
        }
    }

    private fun buildStreamEvent(topic: String, payload: Any) = StreamsEventBuilder()
        .withPayload(payload)
        .withNodeRoutingConfiguration(getStreamsEventSinkStoreEntry()
            .eventRouter
            .eventRouterConfiguration
            .nodeRouting
            .firstOrNull { it.topic == topic })
        .withRelationshipRoutingConfiguration(getStreamsEventSinkStoreEntry()
            .eventRouter
            .eventRouterConfiguration
            .relRouting
            .firstOrNull { it.topic == topic })
        .withTopic(topic)
        .build()

    private fun getStreamsEventSinkStoreEntry() = streamsEventRouterStore[db!!.databaseName()]!!

    companion object {

        private val streamsEventRouterStore = ConcurrentHashMap<String, StreamsEventSinkStoreEntry>()

        fun register(
            db: GraphDatabaseAPI,
            evtRouter: StreamsEventRouter,
            txHandler: StreamsTransactionEventHandler
        ) {
            streamsEventRouterStore[KafkaUtil.getName(db)] = StreamsEventSinkStoreEntry(evtRouter, txHandler)
        }

        fun unregister(db: GraphDatabaseAPI) {
            streamsEventRouterStore.remove(KafkaUtil.getName(db))
        }
    }
}
