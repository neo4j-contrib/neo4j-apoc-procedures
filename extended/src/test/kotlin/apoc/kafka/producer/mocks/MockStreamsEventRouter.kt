package apoc.kafka.producer.mocks

import apoc.kafka.events.StreamsEvent
import apoc.kafka.events.StreamsTransactionEvent
import apoc.kafka.producer.StreamsEventRouter
import apoc.kafka.producer.StreamsEventRouterConfiguration
import apoc.kafka.producer.toMap
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.mockito.Mockito
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import org.neo4j.logging.NullLog

class MockStreamsEventRouter(config: Map<String, String> = emptyMap(),
                             db: GraphDatabaseService = Mockito.mock(GraphDatabaseAPI::class.java),
                             log: Log = NullLog.getInstance()): StreamsEventRouter(config, db, log) {

    override val eventRouterConfiguration: StreamsEventRouterConfiguration = StreamsEventRouterConfiguration()

    private fun fakeRecordMetadata(topic: String) = RecordMetadata(
            TopicPartition(topic, 0),
            0, 1, 2, 3, 4, 5
    ).toMap()

    override fun sendEvents(topic: String, streamsTransactionEvents: List<out StreamsEvent>, config: Map<String, Any?>) {
        events.addAll(streamsTransactionEvents as List<StreamsTransactionEvent>)
    }

    override fun sendEventsSync(topic: String, streamsTransactionEvents: List<out StreamsEvent>, config: Map<String, Any?>): List<Map<String, Any>> {
        val result = mutableListOf<Map<String, Any>>()
        streamsTransactionEvents.forEach {
            result.add(fakeRecordMetadata(topic))
        }
        return result
    }

    override fun start() {}

    override fun stop() {}

    companion object {
        var events = mutableListOf<StreamsTransactionEvent>()

        fun reset() {
            events.clear()
        }
    }

}