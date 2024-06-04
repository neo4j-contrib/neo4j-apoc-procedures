package apoc.kafka.consumer.kafka

import apoc.kafka.PublishProcedures
import apoc.kafka.consumer.procedures.StreamsSinkProcedures
import apoc.kafka.producer.integrations.KafkaEventSinkSuiteIT
import io.confluent.kafka.serializers.KafkaAvroSerializer
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
import org.neo4j.test.TestDatabaseManagementServiceBuilder

import apoc.ExtendedApocConfig.APOC_KAFKA_ENABLED

open class KafkaEventSinkBaseTSE {
    
    companion object {
        private var startedFromSuite = true
        
//        lateinit var db: GraphDatabaseService
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
    lateinit var kafkaAvroProducer: KafkaProducer<GenericRecord, GenericRecord>

    val cypherQueryTemplate = "MERGE (n:Label {id: event.id}) ON CREATE SET n += event.properties"

    // Test data
    val dataProperties = mapOf("prop1" to "foo", "bar" to 1)
    val data = mapOf("id" to 1, "properties" to dataProperties)
    
    @Before
    @BeforeEach
    fun setUp() {
        kafkaProducer = KafkaTestUtils.createProducer(
                bootstrapServers = KafkaEventSinkSuiteIT.kafka.bootstrapServers
        )
        kafkaAvroProducer = KafkaTestUtils.createProducer(
                bootstrapServers = KafkaEventSinkSuiteIT.kafka.bootstrapServers,
                schemaRegistryUrl = KafkaEventSinkSuiteIT.schemaRegistry.getSchemaRegistryUrl(),
                keySerializer = KafkaAvroSerializer::class.java.name,
                valueSerializer = KafkaAvroSerializer::class.java.name)
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
            mutableMapOf
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

//        dbms = TestDatabaseManagementServiceBuilder(temporaryFolder.root.toPath()).build()
//        getDbServices()
        
        if (::kafkaProducer.isInitialized) {
            kafkaProducer.flushAndClose()
        }
        if (::kafkaAvroProducer.isInitialized) {
            kafkaAvroProducer.flushAndClose()
        }
    }
    
    private fun getDbServices(): GraphDatabaseService {
        val db = dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)
        TestUtil.registerProcedure(db, StreamsSinkProcedures::class.java, GlobalProcedures::class.java, PublishProcedures::class.java);
        return db
    }
}