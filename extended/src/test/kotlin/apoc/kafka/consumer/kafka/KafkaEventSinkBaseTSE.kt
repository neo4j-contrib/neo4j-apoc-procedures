package apoc.kafka.consumer.kafka

import apoc.kafka.PublishProcedures
import apoc.kafka.consumer.procedures.StreamsSinkProcedures
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import apoc.kafka.common.support.KafkaTestUtils
import apoc.util.DbmsTestUtil
import apoc.util.TestUtil
import org.junit.*
import org.junit.rules.TemporaryFolder
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.api.procedure.GlobalProcedures

import apoc.ExtendedApocConfig.APOC_KAFKA_ENABLED
import org.apache.kafka.common.serialization.ByteArraySerializer

open class KafkaEventSinkBaseTSE {

    companion object {
        private var startedFromSuite = true

        lateinit var dbms: DatabaseManagementService

        @BeforeClass
        @BeforeAll
        @JvmStatic
        fun setUpContainer() {
            if (!KafkaEventSinkSuiteIT.isRunning) {
                startedFromSuite = false
                KafkaEventSinkSuiteIT.setUpContainer()
            }
        }

        @AfterClass
        @AfterAll
        @JvmStatic
        fun tearDownContainer() {
            if (!startedFromSuite) {
                KafkaEventSinkSuiteIT.tearDownContainer()
            }
        }
    }

    @JvmField
    @Rule
    var temporaryFolder = TemporaryFolder()

    lateinit var kafkaProducer: KafkaProducer<String, ByteArray>
    lateinit var kafkaCustomProducer: KafkaProducer<GenericRecord, GenericRecord>


    // Test data
    val dataProperties = mapOf("prop1" to "foo", "bar" to 1)
    val data = mapOf("id" to 1, "properties" to dataProperties)

    @Before
    @BeforeEach
    fun setUp() {
        kafkaProducer = KafkaTestUtils.createProducer(
            bootstrapServers = KafkaEventSinkSuiteIT.kafka.bootstrapServers
        )
        kafkaCustomProducer = KafkaTestUtils.createProducer(
            bootstrapServers = KafkaEventSinkSuiteIT.kafka.bootstrapServers,
            schemaRegistryUrl = KafkaEventSinkSuiteIT.schemaRegistry.getSchemaRegistryUrl(),
            keySerializer = ByteArraySerializer::class.java.name,
            valueSerializer = ByteArraySerializer::class.java.name)
    }

    fun createDbWithKafkaConfigs(vararg pairs: Pair<String, Any>) : GraphDatabaseService {
        val mutableMapOf = mutableMapOf<String, Any>(
            "apoc.kafka.bootstrap.servers" to KafkaEventSinkSuiteIT.kafka.bootstrapServers,
            APOC_KAFKA_ENABLED to "true",
            "bootstrap.servers" to KafkaEventSinkSuiteIT.kafka.bootstrapServers,
            "apoc.kafka.sink.enabled" to "true"
        )

        mutableMapOf.putAll(mapOf(*pairs))

        dbms = DbmsTestUtil.startDbWithApocConfigs(
            temporaryFolder,
            mutableMapOf as Map<String, Any>?
        )
        return getDbServices()
    }

    private fun <K, V> KafkaProducer<K, V>.flushAndClose() {
        this.flush()
        this.close()
    }

    @After
    @AfterEach
    fun tearDown() {
        dbms.shutdown()

        if (::kafkaProducer.isInitialized) {
            kafkaProducer.flushAndClose()
        }
        if (::kafkaCustomProducer.isInitialized) {
            kafkaCustomProducer.flushAndClose()
        }
    }

    private fun getDbServices(): GraphDatabaseService {
        val db = dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)
        TestUtil.registerProcedure(db, StreamsSinkProcedures::class.java, GlobalProcedures::class.java, PublishProcedures::class.java);
        return db
    }
}