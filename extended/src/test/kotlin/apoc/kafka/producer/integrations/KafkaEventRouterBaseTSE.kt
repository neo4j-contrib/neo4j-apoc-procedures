package apoc.kafka.producer.integrations

import apoc.ExtendedApocConfig.APOC_KAFKA_ENABLED
import apoc.kafka.common.support.KafkaTestUtils
import apoc.kafka.common.support.KafkaTestUtils.getDbServices
import apoc.kafka.common.utils.Neo4jUtilsTest
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
            Neo4jUtilsTest.KAFKA_BOOTSTRAP_SERVER to KafkaEventRouterSuiteIT.kafka.bootstrapServers
        )
        
        mutableMapOf.putAll(mapOf(*pairs))


        dbms = DbmsTestUtil.startDbWithApocConfigs(
            temporaryFolder,
            mutableMapOf
        )

        return getDbServices(dbms)
    }
}