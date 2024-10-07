package apoc.kafka.consumer

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import apoc.kafka.service.TopicType
import apoc.kafka.service.Topics
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

class StreamsTopicService {
    
    private val storage = ConcurrentHashMap<TopicType, Any>()

    private val mutex = Mutex()

    fun clearAll() {
        storage.clear()
    }

    private fun throwRuntimeException(data: Any, topicType: TopicType): Unit =
            throw RuntimeException("Unsupported data $data for topic type $topicType")

    fun set(topicType: TopicType, data: Any) = runBlocking {
        mutex.withLock {
            var oldData = storage[topicType]
            oldData = oldData ?: when (data) {
                is Map<*, *> -> emptyMap<String, Any?>()
                is Collection<*> -> emptyList<String>()
                else -> throwRuntimeException(data, topicType)
            }
            val newData = when (oldData) {
                is Map<*, *> -> oldData + (data as Map<String, Any?>)
                is Collection<*> -> oldData + (data as Collection<String>)
                else -> throwRuntimeException(data, topicType)
            }
            storage[topicType] = newData
        }
    }

    fun remove(topicType: TopicType, topic: String) = runBlocking {
        mutex.withLock {
            val topicData = storage[topicType] ?: return@runBlocking

            val runtimeException = RuntimeException("Unsupported data $topicData for topic type $topicType")
            val filteredData = when (topicData) {
                is Map<*, *> -> topicData.filterKeys { it.toString() != topic }
                is Collection<*> -> topicData.filter { it.toString() != topic }
                else -> throw runtimeException
            }

            storage[topicType] = filteredData
        }
    }

    fun getTopicType(topic: String) = runBlocking {
        TopicType.values()
                .find {
                    mutex.withLock {
                        when (val topicData = storage[it]) {
                            is Map<*, *> -> topicData.containsKey(topic)
                            is Collection<*> -> topicData.contains(topic)
                            else -> false
                        }
                    }
                }
    }

    fun getTopics() = runBlocking {
        TopicType.values()
                .flatMap {
                    mutex.withLock {
                        when (val data = storage[it]) {
                            is Map<*, *> -> data.keys
                            is Collection<*> -> data.toSet()
                            else -> emptySet<String>()
                        }
                    }
                }.toSet() as Set<String>
    }

    fun setAll(topics: Topics) {
        topics.asMap().forEach { (topicType, data) ->
            set(topicType, data)
        }
    }

    fun getCypherTemplate(topic: String) = (storage.getOrDefault(TopicType.CYPHER, emptyMap<String, String>()) as Map<String, String>)
            .let { it[topic] }

    fun getAll(): Map<TopicType, Any> = Collections.unmodifiableMap(storage)

    fun getByTopicType(topicType: TopicType): Any? = storage[topicType]

}