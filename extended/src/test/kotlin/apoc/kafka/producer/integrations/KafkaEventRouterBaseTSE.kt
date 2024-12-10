package apoc.kafka.producer.integrations

import apoc.ExtendedApocConfig.APOC_KAFKA_ENABLED
import apoc.kafka.events.OperationType
import apoc.kafka.events.StreamsTransactionEvent
import apoc.kafka.common.support.KafkaTestUtils
import apoc.kafka.common.support.KafkaTestUtils.getDbServices
import apoc.util.DbmsTestUtil
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.*
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.rules.TemporaryFolder
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.GraphDatabaseService

open class KafkaEventRouterBaseTSE { // TSE (Test Suit Element)

    companion object {

        private var startedFromSuite = true
        lateinit var db: GraphDatabaseService
        lateinit var dbms: DatabaseManagementService

        @BeforeClass
        @JvmStatic
        fun setUpContainer() {
            if (!KafkaEventRouterSuiteIT.isRunning) {
                startedFromSuite = false
                KafkaEventRouterSuiteIT.setUpContainer()
            }
        }

        @AfterClass
        @JvmStatic
        fun tearDownContainer() {
            if (!startedFromSuite) {
                KafkaEventRouterSuiteIT.tearDownContainer()
            }
        }

        // common methods
        fun isValidRelationship(event: StreamsTransactionEvent, type: OperationType) = when (type) {
            OperationType.created -> event.payload.before == null
                    && event.payload.after?.let { it.properties?.let { it.isNullOrEmpty() } } ?: false
                    && event.schema.properties == emptyMap<String, String>()
            OperationType.updated -> event.payload.before?.let { it.properties?.let { it.isNullOrEmpty() } } ?: false
                    && event.payload.after?.let { it.properties == mapOf("type" to "update") } ?: false
                    && event.schema.properties == mapOf("type" to "String")
            OperationType.deleted -> event.payload.before?.let { it.properties == mapOf("type" to "update") } ?: false
                    && event.payload.after == null
                    && event.schema.properties == mapOf("type" to "String")
            else -> throw IllegalArgumentException("Unsupported OperationType")
        }
    }

    lateinit var kafkaConsumer: KafkaConsumer<String, ByteArray>

    @JvmField
    @Rule
    var temporaryFolder = TemporaryFolder()
    
    @Before
    @BeforeEach
    fun setUp() {
        kafkaConsumer = KafkaTestUtils.createConsumer(bootstrapServers = KafkaEventRouterSuiteIT.kafka.bootstrapServers)
    }


    @After
    @AfterEach
    fun tearDown() {
        dbms.shutdown()
        kafkaConsumer.close()
    }

    fun createDbWithKafkaConfigs(vararg pairs: Pair<String, Any>) : GraphDatabaseService {
        val mutableMapOf = mutableMapOf<String, Any>(
            APOC_KAFKA_ENABLED to "true",
            "apoc.kafka.bootstrap.servers" to KafkaEventRouterSuiteIT.kafka.bootstrapServers,
            "bootstrap.servers" to  KafkaEventRouterSuiteIT.kafka.bootstrapServers
        )
        
        mutableMapOf.putAll(mapOf(*pairs))


        dbms = DbmsTestUtil.startDbWithApocConfigs(
            temporaryFolder,
            mutableMapOf
        )

        return getDbServices(dbms)
    }
}