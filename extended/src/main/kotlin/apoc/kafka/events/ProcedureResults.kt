package apoc.kafka.events

class StreamResult(@JvmField val event: Map<String, *>)
class KeyValueResult(@JvmField val name: String, @JvmField val value: Any?)