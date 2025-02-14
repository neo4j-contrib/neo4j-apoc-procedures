package apoc.kafka.consumer

import org.neo4j.logging.Log
import apoc.kafka.service.StreamsSinkEntity


abstract class StreamsEventConsumer(log: Log, topics: Set<Any>) {

    abstract fun stop()

    abstract fun start()

    abstract fun read(topicConfig: Map<String, Any> = emptyMap(), action: (String, List<StreamsSinkEntity>) -> Unit)

    abstract fun read(action: (String, List<StreamsSinkEntity>) -> Unit)

    fun invalidTopics(): List<String> = emptyList()

}


abstract class StreamsEventConsumerFactory {
    abstract fun createStreamsEventConsumer(config: Map<String, String>, log: Log, topics: Set<Any>): StreamsEventConsumer
}