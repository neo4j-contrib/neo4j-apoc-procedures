package apoc.kafka.consumer.kafka

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.SocatContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.util.stream.Stream


class SchemaRegistryContainer(version: String): GenericContainer<SchemaRegistryContainer>("confluentinc/cp-schema-registry:$version") {

    private lateinit var proxy: SocatContainer

    override fun doStart() {
        val networkAlias = networkAliases[0]
        proxy = SocatContainer()
                    .withNetwork(network)
                    .withTarget(PORT, networkAlias)

        proxy.start()
//        waitingFor(Wait.forHttp("/subjects")
//                .forPort(PORT)
//                .forStatusCode(200))
        super.doStart()
    }

    fun withKafka(kafka: KafkaContainer): SchemaRegistryContainer? {
        return kafka.network?.let { withKafka(it, kafka.networkAliases.map { "PLAINTEXT://$it:9092" }.joinToString(",")) }
    }

    fun withKafka(network: Network, bootstrapServers: String): SchemaRegistryContainer {
        withNetwork(network)
        withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
        withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", bootstrapServers)
        return self()
    }

    fun getSchemaRegistryUrl() = "http://${proxy.containerIpAddress}:${proxy.firstMappedPort}"

    override fun stop() {
        Stream.of(Runnable { super.stop() }, Runnable { proxy.stop() }).parallel().forEach { it.run() }
    }

    companion object {
        @JvmStatic val PORT = 8081
    }
}