package apoc.kafka.utils

import apoc.ApocConfig
import apoc.ExtendedApocConfig.APOC_KAFKA_ENABLED
import apoc.kafka.events.Constraint
import apoc.kafka.events.RelKeyStrategy
import apoc.kafka.events.StreamsConstraintType
import apoc.kafka.events.StreamsTransactionEvent
import apoc.kafka.extensions.execute
import apoc.kafka.extensions.quote
import apoc.kafka.service.StreamsSinkEntity
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.config.TopicConfig
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode
import org.neo4j.exceptions.UnsatisfiedDependencyException
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import org.neo4j.logging.internal.LogService
import java.io.IOException
import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType
import java.lang.reflect.Modifier
import java.net.Socket
import java.net.URI
import java.util.*

object KafkaUtil {
    const val labelSeparator = ":"
    const val keySeparator = ", "

    @JvmStatic val UNWIND: String = "UNWIND \$events AS event"

    @JvmStatic val WITH_EVENT_FROM: String = "WITH event, from"

    @JvmStatic val LEADER = "LEADER"

    @JvmStatic val SYSTEM_DATABASE_NAME = "system"

    @JvmStatic
    private val coreMetadata: Class<*>? = try {
        Class.forName("com.neo4j.causalclustering.core.consensus.CoreMetaData")
    } catch (e: ClassNotFoundException) {
        null
    }

    @JvmStatic
    private val isLeaderMethodHandle = coreMetadata?.let {
        val lookup = MethodHandles.lookup()
        lookup.findVirtual(it, "isLeader", MethodType.methodType(Boolean::class.java))
            .asType(MethodType.methodType(Boolean::class.java, Any::class.java))
    }

    fun clusterMemberRole(db: GraphDatabaseAPI): String {
        val fallback: (Exception?) -> String = { e: Exception? ->
            val userLog = db.dependencyResolver
                .resolveDependency(LogService::class.java)
                .getUserLog(KafkaUtil::class.java)
            e?.let { userLog.warn("Cannot call the APIs, trying with the cypher query", e) }
                ?: userLog.warn("Cannot call the APIs, trying with the cypher query")
            db.execute("CALL dbms.cluster.role(\$database)",
                mapOf("database" to db.databaseName())
            ) { it.columnAs<String>("role").next() }
        }
        val execute = {
            coreMetadata?.let {
                try {
                    val raftMachine: Any = db.dependencyResolver.resolveDependency(coreMetadata)
                    val isLeader = isLeaderMethodHandle!!.invokeExact(raftMachine) as Boolean
                    if (isLeader) "LEADER" else "FOLLOWER"
                } catch (e: UnsatisfiedDependencyException) {
                    "LEADER"
                }
            } ?: "LEADER"
        }
        return executeOrFallback(execute, fallback)
    }

    fun isCluster(db: GraphDatabaseAPI): Boolean = db.mode() != HostedOnMode.SINGLE && db.mode() != HostedOnMode.VIRTUAL

    fun isCluster(dbms: DatabaseManagementService): Boolean = dbms.listDatabases()
        .firstOrNull { it != KafkaUtil.SYSTEM_DATABASE_NAME }
        ?.let { dbms.database(it) as GraphDatabaseAPI }
        ?.let { isCluster(it) } ?: false

    private fun <T> executeOrFallback(execute: () -> T, fallback: (Exception?) -> T): T = try {
        execute()
    } catch (e: Exception) {
        fallback(e)
    }

    fun getLabelsAsString(labels: Collection<String>): String = labels
        .map { it.quote() }
        .joinToString(labelSeparator)
        .let { if (it.isNotBlank()) "$labelSeparator$it" else it }

    fun getNodeKeysAsString(prefix: String = "properties", keys: Set<String>): String = keys
        .map { toQuotedProperty(prefix, it) }
        .joinToString(keySeparator)

    private fun toQuotedProperty(prefix: String = "properties", property: String): String {
        val quoted = property.quote()
        return "$quoted: event.$prefix.$quoted"
    }

    fun getNodeMergeKeys(prefix: String, keys: Set<String>): String = keys
        .map {
            val quoted = it.quote()
            "$quoted: event.$prefix.$quoted"
        }
        .joinToString(keySeparator)

    fun containsProp(key: String, properties: List<String>): Boolean = if (key.contains(".")) {
        properties.contains(key) || properties.any { key.startsWith("$it.") }
    } else {
        properties.contains(key)
    }

    suspend fun <T> retryForException(exceptions: Array<Class<out Throwable>>, retries: Int, delayTime: Long, action: () -> T): T {
        return try {
            action()
        } catch (e: Exception) {
            val isInstance = exceptions.any { it.isInstance(e) }
            if (isInstance && retries > 0) {
                delay(delayTime)
                retryForException(exceptions = exceptions, retries = retries - 1, delayTime = delayTime, action = action)
            } else {
                throw e
            }
        }
    }

    fun isServerReachable(url: String, port: Int): Boolean = try {
        Socket(url, port).use { true }
    } catch (e: IOException) {
        false
    }

    fun checkServersUnreachable(urls: String, separator: String = ","): List<String> = urls
        .split(separator)
        .map {
            val uri = URI.create(it)
            when (uri.host.isNullOrBlank()) {
                true -> {
                    val splitted = it.split(":")
                    URI("fake-scheme", "", splitted.first(), splitted.last().toInt(),
                        "", "", "")
                }
                else -> uri
            }
        }
        .filter { uri -> !isServerReachable(uri.host, uri.port) }
        .map { if (it.scheme == "fake-scheme") "${it.host}:${it.port}" else it.toString() }

    fun validateConnection(url: String, kafkaPropertyKey: String, checkReachable: Boolean = true) {
        if (url.isBlank()) {
            throw RuntimeException("The `kafka.$kafkaPropertyKey` property is empty")
        } else if (checkReachable) {
            val unreachableServers = checkServersUnreachable(url)
            if (unreachableServers.isNotEmpty()) {
                throw RuntimeException("The servers defined into the property `kafka.$kafkaPropertyKey` are not reachable: $unreachableServers")
            }
        }
    }

    fun getInvalidTopicsError(invalidTopics: List<String>) = "The BROKER config `auto.create.topics.enable` is false, the following topics need to be created into the Kafka cluster otherwise the messages will be discarded: $invalidTopics"

    fun getInvalidTopics(kafkaProps: Properties, allTopics: List<String>): List<String> = try {
        getInvalidTopics(AdminClient.create(kafkaProps), allTopics)
    } catch (e: Exception) {
        emptyList()
    }

    fun getInvalidTopics(client: AdminClient, allTopics: List<String>): List<String> = try {
        val kafkaTopics = client.listTopics().names().get()
        val invalidTopics = allTopics.filter { !kafkaTopics.contains(it) }
        if (invalidTopics.isNotEmpty() && isAutoCreateTopicsEnabled(client)) {
            emptyList()
        } else {
            invalidTopics
        }
    } catch (e: Exception) {
        emptyList()
    }

    fun checkEnabled() {
        if (!ApocConfig.apocConfig().getBoolean(APOC_KAFKA_ENABLED))  {
            throw RuntimeException("In order to use the Kafka procedures you must set ${APOC_KAFKA_ENABLED}=true")
        }
    }

    fun isAutoCreateTopicsEnabled(kafkaProps: Properties):Boolean = try {
        isAutoCreateTopicsEnabled(AdminClient.create(kafkaProps))
    } catch (e: Exception) {
        false
    }

    fun isAutoCreateTopicsEnabled(client: AdminClient): Boolean = try {
        val firstNodeId = client.describeCluster().nodes().get().first().id()
        val configResources = listOf(ConfigResource(ConfigResource.Type.BROKER, firstNodeId.toString()))
        val configs = client.describeConfigs(configResources).all().get()
        configs.values
            .flatMap { it.entries() }
            .find { it.name() == "auto.create.topics.enable" }
            ?.value()
            ?.toBoolean() ?: false
    } catch (e: Exception) {
        false
    }

    private fun getConfigProperties(clazz: Class<*>) = clazz.declaredFields
        .filter { Modifier.isStatic(it.modifiers) && it.name.endsWith("_CONFIG") }
        .map { it.get(null).toString() }
        .toSet()

    private fun getBaseConfigs() = (getConfigProperties(CommonClientConfigs::class.java)
            + AdminClientConfig.configNames()
            + getConfigProperties(SaslConfigs::class.java)
            + getConfigProperties(TopicConfig::class.java)
            + getConfigProperties(SslConfigs::class.java))

    fun getProducerProperties() = ProducerConfig.configNames() - getBaseConfigs()

    fun getConsumerProperties() = ConsumerConfig.configNames() - getBaseConfigs()

    fun getNodeKeys(labels: List<String>, propertyKeys: Set<String>, constraints: List<Constraint>, keyStrategy: RelKeyStrategy = RelKeyStrategy.DEFAULT): Set<String> =
        constraints
            .filter { constraint ->
                constraint.type == StreamsConstraintType.UNIQUE
                        && propertyKeys.containsAll(constraint.properties)
                        && labels.contains(constraint.label)
            }
            .let {
                when(keyStrategy) {
                    RelKeyStrategy.DEFAULT -> {
                        // we order first by properties.size, then by label name and finally by properties name alphabetically
                        // with properties.sorted() we ensure that ("foo", "bar") and ("bar", "foo") are no different
                        // with toString() we force it.properties to have the natural sort order, that is alphabetically
                        it.minWithOrNull((compareBy({ it.properties.size }, { it.label }, { it.properties.sorted().toString() })))
                            ?.properties
                            .orEmpty()
                    }
                    // with 'ALL' strategy we get a set with all properties
                    RelKeyStrategy.ALL -> it.flatMap { it.properties }.toSet()
                }
            }


    fun toStreamsTransactionEvent(streamsSinkEntity: StreamsSinkEntity,
                                  evaluation: (StreamsTransactionEvent) -> Boolean)
            : StreamsTransactionEvent? = if (streamsSinkEntity.value != null) {
        val data = JSONUtils.asStreamsTransactionEvent(streamsSinkEntity.value)
        if (evaluation(data)) data else null
    } else {
        null
    }

    fun <T> ignoreExceptions(action: () -> T, vararg toIgnore: Class<out Throwable>): T? {
        return try {
            action()
        } catch (e: Throwable) {
            if (toIgnore.isEmpty()) {
                return null
            }
            return if (toIgnore.any { it.isInstance(e) }) {
                null
            } else {
                throw e
            }
        }
    }

    fun blockUntilFalseOrTimeout(timeout: Long, delay: Long = 1000, action: () -> Boolean): Boolean = runBlocking {
        val start = System.currentTimeMillis()
        var success = action()
        while (System.currentTimeMillis() - start < timeout && !success) {
            delay(delay)
            success = action()
        }
        success
    }

    fun getName(db: GraphDatabaseService) = db.databaseName()
    
    fun isWriteableInstance(db: GraphDatabaseAPI) = apoc.util.Util.isWriteableInstance(db)

    private fun clusterHasLeader(db: GraphDatabaseAPI): Boolean = try {
        db.execute("""
                |CALL dbms.cluster.overview() YIELD databases
                |RETURN databases[${'$'}database] AS role
            """.trimMargin(), mapOf("database" to db.databaseName())) {
            it.columnAs<String>("role")
                .stream()
                .toList()
                .contains(KafkaUtil.LEADER)
        }
    } catch (e: QueryExecutionException) {
        if (e.statusCode.equals("Neo.ClientError.Procedure.ProcedureNotFound", ignoreCase = true)) {
            false
        }
        throw e
    }

    fun <T> executeInWriteableInstance(db: GraphDatabaseAPI,
                                       action: () -> T?): T? = if (isWriteableInstance(db)) {
        action()
    } else {
        null
    }

    fun isClusterCorrectlyFormed(dbms: DatabaseManagementService) = dbms.listDatabases()
        .filterNot { it == KafkaUtil.SYSTEM_DATABASE_NAME }
        .map { dbms.database(it) as GraphDatabaseAPI }
        .all { clusterHasLeader(it) }

    fun waitForTheLeaders(dbms: DatabaseManagementService, log: Log, timeout: Long = 120000, action: () -> Unit) {
        GlobalScope.launch(Dispatchers.IO) {
            val start = System.currentTimeMillis()
            val delay: Long = 2000
            while (!isClusterCorrectlyFormed(dbms) && System.currentTimeMillis() - start < timeout) {
                log.info("${KafkaUtil.LEADER} not found, new check comes in $delay milliseconds...")
                delay(delay)
            }
            action()
        }
    }
}