package apoc.kafka.producer.integrations

import apoc.kafka.consumer.kafka.SchemaRegistryContainer
import apoc.kafka.utils.KafkaUtil
import org.junit.AfterClass
import org.junit.Assume.assumeTrue
import org.junit.BeforeClass
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Network
import org.testcontainers.utility.DockerImageName

class KafkaEventSinkSuiteIT {
    companion object {
        /**
         * Kafka TestContainers uses Confluent OSS images.
         * We need to keep in mind which is the right Confluent Platform version for the Kafka version this project uses
         *
         * Confluent Platform | Apache Kafka
         *                    |
         * 4.0.x	          | 1.0.x
         * 4.1.x	          | 1.1.x
         * 5.0.x	          | 2.0.x
         *
         * Please see also https://docs.confluent.io/current/installation/versions-interoperability.html#cp-and-apache-kafka-compatibility
         */
        private const val confluentPlatformVersion = "7.6.2"
        @JvmStatic lateinit var kafka: KafkaContainer
        @JvmStatic lateinit var schemaRegistry: SchemaRegistryContainer

        var isRunning = false

        @BeforeClass
        @JvmStatic
        fun setUpContainer() {
                kafka = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.2"))
                    .withNetwork(Network.newNetwork())
                kafka.start()
                schemaRegistry = SchemaRegistryContainer(confluentPlatformVersion)
                    .withExposedPorts(8081)
                    .dependsOn(kafka)
                    .withKafka(kafka)!!
                schemaRegistry.start()
                isRunning = true
            assumeTrue("Kafka must be running", ::kafka.isInitialized && kafka.isRunning)
            assumeTrue("Schema Registry must be running", schemaRegistry.isRunning)
            assumeTrue("isRunning must be true", isRunning)
        }

        @AfterClass
        @JvmStatic
        fun tearDownContainer() {
            KafkaUtil.ignoreExceptions({
                kafka.stop()
                schemaRegistry.stop()
                isRunning = false
            }, UninitializedPropertyAccessException::class.java)
        }
    }
}