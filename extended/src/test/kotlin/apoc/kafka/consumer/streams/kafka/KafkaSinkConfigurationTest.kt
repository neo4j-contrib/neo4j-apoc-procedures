//package streams.kafka
//
////import apoc.kafka.consumer.StreamsSinkConfiguration
//import apoc.kafka.consumer.kafka.KafkaSinkConfiguration
//import io.confluent.kafka.serializers.KafkaAvroDeserializer
//import org.apache.kafka.clients.consumer.ConsumerConfig
//import org.apache.kafka.common.serialization.ByteArrayDeserializer
//import org.junit.Ignore
//import org.junit.Test
////import apoc.kafka.consumer.streams.StreamsSinkConfigurationTest
//import apoc.kafka.service.TopicValidationException
//import kotlin.test.assertEquals
//import kotlin.test.assertTrue
//
//class KafkaSinkConfigurationTest {
//
//    private val defaultDbName = "neo4j"
//
//    @Test
//    fun `should return default configuration`() {
//        val default = KafkaSinkConfiguration()
//        StreamsSinkConfigurationTest.testDefaultConf(default.sinkConfiguration)
//
//        assertEquals("localhost:9092", default.bootstrapServers)
//        assertEquals("neo4j", default.groupId)
//        assertEquals("earliest", default.autoOffsetReset)
//        assertEquals(ByteArrayDeserializer::class.java.name, default.keyDeserializer)
//        assertEquals(ByteArrayDeserializer::class.java.name, default.valueDeserializer)
//        assertEquals(true, default.enableAutoCommit)
//        assertEquals(false, default.asyncCommit)
//        assertEquals(emptyMap(), default.extraProperties)
//    }
//
//    @Test
//    fun `should return configuration from map`() {
//        val topic = "topic-neo"
//        val topicKey = "apoc.kafka.sink.topic.cypher.$topic"
//        val topicValue = "MERGE (n:Label{ id: event.id }) "
//        val bootstrap = "bootstrap:9092"
//        val group = "foo"
//        val autoOffsetReset = "latest"
//        val autoCommit = "false"
//        val config = mapOf(topicKey to topicValue,
//                "apoc.kafka.bootstrap.servers" to bootstrap,
//                "apoc.kafka.auto.offset.reset" to autoOffsetReset,
//                "apoc.kafka.enable.auto.commit" to autoCommit,
//                "apoc.kafka.group.id" to group,
//                "apoc.kafka.async.commit" to "true",
//                "apoc.kafka.key.deserializer" to ByteArrayDeserializer::class.java.name,
//                "apoc.kafka.value.deserializer" to KafkaAvroDeserializer::class.java.name)
//        val expectedMap = mapOf("bootstrap.servers" to bootstrap,
//                "auto.offset.reset" to autoOffsetReset, "enable.auto.commit" to autoCommit, "group.id" to group,
//                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java.toString(),
//                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java.toString(),
//                "async.commit" to "true",
//                "key.deserializer" to ByteArrayDeserializer::class.java.name,
//                "value.deserializer" to KafkaAvroDeserializer::class.java.name)
//
//        val kafkaSinkConfiguration = KafkaSinkConfiguration.create(config, defaultDbName, isDefaultDb = true)
//        StreamsSinkConfigurationTest.testFromConf(kafkaSinkConfiguration.sinkConfiguration, topic, topicValue)
//        assertEquals(emptyMap(), kafkaSinkConfiguration.extraProperties)
//        assertEquals(bootstrap, kafkaSinkConfiguration.bootstrapServers)
//        assertEquals(autoOffsetReset, kafkaSinkConfiguration.autoOffsetReset)
//        assertEquals(group, kafkaSinkConfiguration.groupId)
//        val resultMap = kafkaSinkConfiguration
//                .asProperties()
//                .map { it.key.toString() to it.value.toString() }
//                .toMap()
//        assertEquals(expectedMap, resultMap)
//
//        val streamsConfig = StreamsSinkConfiguration.from(config, defaultDbName, isDefaultDb = true)
//        assertTrue { streamsConfig.topics.cypherTopics.containsKey(topic) }
//        assertEquals(topicValue, streamsConfig.topics.cypherTopics[topic])
//    }
//
//    @Test
//    fun `should return configuration from map for non default DB`() {
//        val dbName = "foo"
//        val topic = "topic-neo"
//        val topicKey = "apoc.kafka.sink.topic.cypher.$topic"
//        val topicValue = "MERGE (n:Label{ id: event.id })"
//        val topicKeyFoo = "apoc.kafka.sink.topic.cypher.$topic.to.foo"
//        val topicValueFoo = "MERGE (n:Foo{ id: event.id })"
//        val bootstrap = "bootstrap:9092"
//        val group = "mygroup"
//        val autoOffsetReset = "latest"
//        val autoCommit = "false"
//        val asyncCommit = "true"
//        val config = mapOf(topicKey to topicValue,
//                topicKeyFoo to topicValueFoo,
//                "apoc.kafka.bootstrap.servers" to bootstrap,
//                "apoc.kafka.auto.offset.reset" to autoOffsetReset,
//                "apoc.kafka.enable.auto.commit" to autoCommit,
//                "apoc.kafka.group.id" to group,
//                "apoc.kafka.async.commit" to asyncCommit,
//                "apoc.kafka.key.deserializer" to ByteArrayDeserializer::class.java.name,
//                "apoc.kafka.value.deserializer" to KafkaAvroDeserializer::class.java.name)
//        val expectedMap = mapOf("bootstrap.servers" to bootstrap,
//                "auto.offset.reset" to autoOffsetReset, "enable.auto.commit" to autoCommit, "group.id" to "$group-$dbName",
//                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java.toString(),
//                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java.toString(),
//                "key.deserializer" to ByteArrayDeserializer::class.java.name,
//                "apoc.kafka.async.commit" to asyncCommit,
//                "value.deserializer" to KafkaAvroDeserializer::class.java.name)
//
//        val kafkaSinkConfiguration = KafkaSinkConfiguration.create(config, dbName, isDefaultDb = false)
//        StreamsSinkConfigurationTest.testFromConf(kafkaSinkConfiguration.sinkConfiguration, topic, topicValueFoo)
//        assertEquals(emptyMap(), kafkaSinkConfiguration.extraProperties)
//        assertEquals(bootstrap, kafkaSinkConfiguration.bootstrapServers)
//        assertEquals(autoOffsetReset, kafkaSinkConfiguration.autoOffsetReset)
//        assertTrue { kafkaSinkConfiguration.asyncCommit }
//        assertEquals("$group-$dbName", kafkaSinkConfiguration.groupId)
//        val resultMap = kafkaSinkConfiguration
//                .asProperties()
//                .map { it.key.toString() to it.value.toString() }
//                .toMap()
//        assertEquals(expectedMap, resultMap)
//
//        val streamsConfig = StreamsSinkConfiguration.from(config, dbName, isDefaultDb = false)
//        assertEquals(1, streamsConfig.topics.cypherTopics.size)
//        assertTrue { streamsConfig.topics.cypherTopics.containsKey(topic) }
//        assertEquals(topicValueFoo, streamsConfig.topics.cypherTopics[topic])
//    }
//
//    @Test(expected = TopicValidationException::class)
//    @Ignore("Disabled, use Kafka to deal with availability of the configured services")
//    fun `should not validate the configuration because of unreachable kafka bootstrap server`() {
//        val bootstrap = "bootstrap:9092"
//        try {
//            val topic = "topic-neo"
//            val topicKey = "apoc.kafka.sink.topic.cypher.$topic"
//            val topicValue = "MERGE (n:Label{ id: event.id }) "
//            val group = "foo"
//            val autoOffsetReset = "latest"
//            val autoCommit = "false"
//            val config = mapOf(topicKey to topicValue,
//                    "$topicKey.to.foo" to "$topicValue SET n += event.properties",
//                    "apoc.kafka.bootstrap.servers" to bootstrap,
//                    "apoc.kafka.auto.offset.reset" to autoOffsetReset,
//                    "apoc.kafka.enable.auto.commit" to autoCommit,
//                    "apoc.kafka.group.id" to group,
//                    "apoc.kafka.key.deserializer" to ByteArrayDeserializer::class.java.name,
//                    "apoc.kafka.value.deserializer" to KafkaAvroDeserializer::class.java.name)
//            KafkaSinkConfiguration.from(config, defaultDbName, isDefaultDb = true)
//        } catch (e: TopicValidationException) {
//            assertEquals("The servers defined into the property `kafka.bootstrap.servers` are not reachable: [$bootstrap]", e.message)
//            throw e
//        }
//    }
//
//    @Test(expected = RuntimeException::class)
//    fun `should not validate the configuration because of empty kafka bootstrap server`() {
//        val bootstrap = ""
//        try {
//            val topic = "topic-neo"
//            val topicKey = "apoc.kafka.sink.topic.cypher.$topic"
//            val topicValue = "MERGE (n:Label{ id: event.id }) "
//            val group = "foo"
//            val autoOffsetReset = "latest"
//            val autoCommit = "false"
//            val config = mapOf(topicKey to topicValue,
//                    "apoc.kafka.bootstrap.servers" to bootstrap,
//                    "apoc.kafka.auto.offset.reset" to autoOffsetReset,
//                    "apoc.kafka.enable.auto.commit" to autoCommit,
//                    "apoc.kafka.group.id" to group,
//                    "apoc.kafka.key.deserializer" to ByteArrayDeserializer::class.java.name,
//                    "apoc.kafka.value.deserializer" to KafkaAvroDeserializer::class.java.name)
//            KafkaSinkConfiguration.from(config, defaultDbName, isDefaultDb = true)
//        } catch (e: RuntimeException) {
//            assertEquals("The `kafka.bootstrap.servers` property is empty", e.message)
//            throw e
//        }
//    }
//
//}