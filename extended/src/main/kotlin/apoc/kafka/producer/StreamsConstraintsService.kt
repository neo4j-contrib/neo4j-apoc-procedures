package apoc.kafka.producer

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.neo4j.graphdb.DatabaseShutdownException
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.TransactionFailureException
import apoc.kafka.events.Constraint
import apoc.kafka.utils.KafkaUtil
import java.io.Closeable
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

class StreamsConstraintsService(private val db: GraphDatabaseService, private val poolInterval: Long): Closeable {

    private val nodeConstraints = ConcurrentHashMap<String, Set<Constraint>>()
    private val relConstraints = ConcurrentHashMap<String, Set<Constraint>>()

    private lateinit var job: Job

    override fun close() {
        KafkaUtil.ignoreExceptions({ runBlocking { job.cancelAndJoin() } }, UninitializedPropertyAccessException::class.java)
    }

    fun start() {
        job = GlobalScope.launch(Dispatchers.IO) {
            while (isActive) {
                if (!db.isAvailable(5000)) return@launch
                KafkaUtil.ignoreExceptions({
                    db.beginTx().use {
                        val constraints = it.schema().constraints
                        constraints
                                .filter { it.isNodeConstraint() }
                                .groupBy { it.label.name() }
                                .forEach { label, constraints ->
                                    nodeConstraints[label] = constraints
                                            .map { Constraint(label, it.propertyKeys.toSet(), it.streamsConstraintType()) }
                                            .toSet()
                                }
                        constraints
                                .filter { it.isRelationshipConstraint() }
                                .groupBy { it.relationshipType.name() }
                                .forEach { relationshipType, constraints ->
                                    relConstraints[relationshipType] = constraints
                                            .map { Constraint(relationshipType, it.propertyKeys.toSet(), it.streamsConstraintType()) }
                                            .toSet()
                                }
                    }
                }, DatabaseShutdownException::class.java, TransactionFailureException::class.java, IllegalStateException::class.java)
                delay(poolInterval)
            }
        }
    }

    fun forLabel(label: Label): Set<Constraint> {
        return nodeConstraints[label.name()] ?: emptySet()
    }

    fun forRelationshipType(relationshipType: RelationshipType): Set<Constraint> {
        return relConstraints[relationshipType.name()] ?: emptySet()
    }

    fun allForLabels(): Map<String, Set<Constraint>> {
        return Collections.unmodifiableMap(nodeConstraints)
    }

    fun allForRelationshipType(): Map<String, Set<Constraint>> {
        return Collections.unmodifiableMap(relConstraints)
    }


}